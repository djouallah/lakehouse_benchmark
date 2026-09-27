SELECT
    --Query01
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    `{schema}.lineitem`
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;



WITH cheapest_part AS (
    SELECT
        MIN(ps.ps_supplycost) AS cp_lowest,
        p.p_partkey AS cp_partkey
    FROM `{schema}.part` p
    JOIN `{schema}.partsupp` ps ON p.p_partkey = ps.ps_partkey
    JOIN `{schema}.supplier` s ON s.s_suppkey = ps.ps_suppkey
    JOIN `{schema}.nation` n ON s.s_nationkey = n.n_nationkey
    JOIN `{schema}.region` r ON n.n_regionkey = r.r_regionkey
    WHERE r.r_name = 'EUROPE'
    GROUP BY p.p_partkey
)
SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM `{schema}.part` p
JOIN `{schema}.partsupp` ps ON p.p_partkey = ps.ps_partkey
JOIN `{schema}.supplier` s ON s.s_suppkey = ps.ps_suppkey
JOIN `{schema}.nation` n ON s.s_nationkey = n.n_nationkey
JOIN `{schema}.region` r ON n.n_regionkey = r.r_regionkey
JOIN cheapest_part cp ON ps.ps_supplycost = cp.cp_lowest AND cp.cp_partkey = p.p_partkey
WHERE p.p_size = 15
  AND p.p_type LIKE '%BRASS'
  AND r.r_name = 'EUROPE'
ORDER BY s.s_acctbal DESC,
         n.n_name,
         s.s_name,
         p.p_partkey
LIMIT 10;



SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    `{schema}.customer` c
JOIN `{schema}.orders` o ON c.c_custkey = o.o_custkey
JOIN `{schema}.lineitem` l ON l.l_orderkey = o.o_orderkey
WHERE
    c.c_mktsegment = 'BUILDING'
    AND o.o_orderdate < CAST('1995-03-15' AS DATE)
    AND l.l_shipdate > CAST('1995-03-15' AS DATE)
GROUP BY
    l.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate
LIMIT 10;



select
--Query04
    o_orderpriority,
    count(*) as order_count
from
    `{schema}.orders`
where
    o_orderdate >= cast('1993-07-01' as date)
    and o_orderdate < cast('1993-10-01' as date)
    and o_orderkey in (
        select
            l_orderkey
        from
            `{schema}.lineitem`
        where
            l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;




SELECT
    --Query05
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM `{schema}.lineitem`
inner join (select * from `{schema}.orders` where o_orderdate >= CAST('1994-01-01' AS date) AND o_orderdate < CAST('1995-01-01' AS date)) as x
on l_orderkey = x.o_orderkey
left join `{schema}.supplier`
on l_suppkey = s_suppkey
left join `{schema}.customer`
on o_custkey = c_custkey and
c_nationkey = s_nationkey
left join `{schema}.nation`
on s_nationkey = n_nationkey
inner join ( select * from `{schema}.region` where r_name = 'ASIA') as xx
on n_regionkey = xx.r_regionkey

GROUP BY
    n_name
ORDER BY
    revenue DESC;


SELECT
    --Query06
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    `{schema}.lineitem`
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;







SELECT
    --Query07
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        EXTRACT(YEAR FROM l.l_shipdate) AS l_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume
    FROM
        `{schema}.supplier` s
    JOIN `{schema}.lineitem` l ON s.s_suppkey = l.l_suppkey
    JOIN `{schema}.orders` o ON o.o_orderkey = l.l_orderkey
    JOIN `{schema}.customer` c ON c.c_custkey = o.o_custkey
    JOIN `{schema}.nation` n1 ON s.s_nationkey = n1.n_nationkey
    JOIN `{schema}.nation` n2 ON c.c_nationkey = n2.n_nationkey
    WHERE
        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        AND l.l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;








SELECT
    --Query08
        EXTRACT( year  FROM  o_orderdate ) AS o_year,
        SUM(  CASE  WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0  END ) / SUM(l_extendedprice * (1 - l_discount)) AS mkt_share
        FROM  `{schema}.lineitem`
        inner join   (select o_custkey,o_orderdate, o_orderkey from  `{schema}.orders` WHERE  o_orderdate BETWEEN CAST('1995-01-01' AS date) AND CAST('1996-12-31' AS date) ) xxx
        on l_orderkey = xxx.o_orderkey
        inner join  (select p_partkey from  `{schema}.part`  where p_type = 'ECONOMY ANODIZED STEEL' ) z
        on  l_partkey = z.p_partkey
        left join    `{schema}.supplier`
        on  l_suppkey = s_suppkey
        left join   `{schema}.customer`
        on o_custkey = c_custkey
        left join   `{schema}.nation` n1
        on c_nationkey = n1.n_nationkey
        left join   `{schema}.nation` n2
        on s_nationkey = n2.n_nationkey
        inner join  (select * from `{schema}.region` where r_name = 'AMERICA') cc
        on  n1.n_regionkey = cc.r_regionkey




GROUP BY
    EXTRACT( year  FROM  o_orderdate )
ORDER BY
    o_year;










SELECT
    --Query09
    n_name AS nation,
    EXTRACT( year  FROM o_orderdate ) AS o_year,
    sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
        FROM `{schema}.lineitem`
        inner join ( select p_partkey from `{schema}.part` where  p_name LIKE '%green%') xx
        on  l_partkey = xx.p_partkey
        left join `{schema}.orders`
        on  l_orderkey =o_orderkey
        left join  `{schema}.partsupp`
        on  l_suppkey =ps_suppkey  AND  l_partkey = ps_partkey
        left join  `{schema}.supplier`
        on    l_suppkey =s_suppkey
        left join `{schema}.nation`
        on  n_nationkey = s_nationkey


GROUP BY
    n_name,
    EXTRACT( year  FROM o_orderdate )
ORDER BY
    n_name,
    o_year DESC;



SELECT
    --Query10
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM  `{schema}.lineitem`
inner join ( select * from `{schema}.orders` where o_orderdate >= CAST('1993-10-01' AS date) AND o_orderdate < CAST('1994-01-01' AS date)) as xx
on l_orderkey = xx.o_orderkey
left join `{schema}.customer`
on xx.o_custkey = c_custkey
left join `{schema}.nation`
on c_nationkey = n_nationkey
WHERE  l_returnflag = 'R'

GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT
    20;



WITH germany_value AS (
    SELECT
        SUM(ps.ps_supplycost * ps.ps_availqty) * (0.0001 / {SF}) AS threshold
    FROM
        `{schema}.partsupp` ps
    JOIN `{schema}.supplier` s ON ps.ps_suppkey = s.s_suppkey
    JOIN `{schema}.nation` n ON s.s_nationkey = n.n_nationkey
    WHERE
        n.n_name = 'GERMANY'
),
partkey_values AS (
    SELECT
        ps.ps_partkey,
        SUM(ps.ps_supplycost * ps.ps_availqty) AS value
    FROM
        `{schema}.partsupp` ps
    JOIN `{schema}.supplier` s ON ps.ps_suppkey = s.s_suppkey
    JOIN `{schema}.nation` n ON s.s_nationkey = n.n_nationkey
    WHERE
        n.n_name = 'GERMANY'
    GROUP BY
        ps.ps_partkey
)
SELECT pv.ps_partkey, pv.value
FROM partkey_values pv
CROSS JOIN germany_value gv
WHERE pv.value > gv.threshold
ORDER BY pv.value DESC;







SELECT
    --Query12
    l_shipmode,
    SUM(
        CASE
            WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    SUM(
        CASE
            WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS low_line_count
FROM `{schema}.lineitem`
left join  `{schema}.orders`
on o_orderkey = l_orderkey

WHERE  l_shipmode IN ('MAIL', 'SHIP')
       AND l_commitdate < l_receiptdate
       AND l_shipdate < l_commitdate
       AND l_receiptdate >= CAST('1994-01-01' AS date) AND l_receiptdate < CAST('1995-01-01' AS date)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;








--Query13
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c_custkey,
            COUNT(o_orderkey) AS c_count
        FROM
            `{schema}.customer`
            LEFT OUTER JOIN (
                SELECT o_custkey, o_orderkey
                FROM `{schema}.orders`
                WHERE o_comment NOT LIKE '%special%requests%'
            ) AS filtered_orders ON c_custkey = o_custkey
        GROUP BY
            c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;








SELECT
    --Query14
    100.00 * SUM(
        CASE
            WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
            ELSE 0
        END
    ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM  `{schema}.lineitem`
left join `{schema}.part`
on l_partkey = p_partkey
WHERE l_shipdate >= cast('1995-09-01' as date) AND l_shipdate < cast('1995-10-01' as date);








--Query15
WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        `{schema}.lineitem`
    WHERE
        l_shipdate >= CAST('1996-01-01' AS date)
        AND l_shipdate < CAST('1996-04-01' AS date)
    GROUP BY
        l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    `{schema}.supplier`
JOIN revenue ON s_suppkey = supplier_no
WHERE
    total_revenue IN (
        SELECT MAX(total_revenue)
        FROM revenue
    )
ORDER BY
    s_suppkey;







SELECT
    --Query16
    p.p_brand,
    p.p_type,
    p.p_size,
    COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM
    `{schema}.partsupp` ps
JOIN `{schema}.part` p ON p.p_partkey = ps.ps_partkey
WHERE
    p.p_brand <> 'Brand#45'
    AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps.ps_suppkey NOT IN (
        SELECT
            s.s_suppkey
        FROM
            `{schema}.supplier` s
        WHERE
            s.s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p.p_brand,
    p.p_type,
    p.p_size;









WITH part_avg AS (
    -- Query17
    SELECT
        (0.2 * AVG(l.l_quantity)) AS limit_qty,
        l.l_partkey AS lpk
    FROM `{schema}.lineitem` l
    GROUP BY l.l_partkey
)
SELECT
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    `{schema}.lineitem` l
JOIN `{schema}.part` p ON p.p_partkey = l.l_partkey
JOIN part_avg pa ON p.p_partkey = pa.lpk
WHERE
    p.p_brand = 'Brand#23'
    AND p.p_container = 'MED BOX'
    AND l.l_quantity < pa.limit_qty;







SELECT
    --Query18
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity)
FROM
    `{schema}.customer` c
JOIN `{schema}.orders` o ON c.c_custkey = o.o_custkey
JOIN `{schema}.lineitem` l ON o.o_orderkey = l.l_orderkey
WHERE
    o.o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            `{schema}.lineitem`
        GROUP BY
            l_orderkey
        HAVING
            SUM(l_quantity) > 300
    )
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate
LIMIT
    100;






select
--Query19
sum(l_extendedprice* (1 - l_discount)) as revenue

from `{schema}.lineitem`
join  `{schema}.part`
ON p_partkey = l_partkey
where (
        p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    ) or ( p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    ) or ( p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );





--Query20
WITH availability_part_supp AS (
    SELECT 
        0.5 * SUM(l_quantity) AS ps_halfqty, 
        l_partkey AS pkey, 
        l_suppkey AS skey
    FROM `{schema}.lineitem`
    WHERE l_shipdate >= CAST('1994-01-01' AS date)
      AND l_shipdate < CAST('1995-01-01' AS date)
    GROUP BY l_partkey, l_suppkey
)
SELECT s_name, s_address
FROM `{schema}.supplier`
JOIN `{schema}.nation` ON s_nationkey = n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey
    FROM `{schema}.partsupp`
    JOIN availability_part_supp ON ps_partkey = pkey AND ps_suppkey = skey
    WHERE ps_partkey IN (
        SELECT p_partkey
        FROM `{schema}.part`
        WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > ps_halfqty
)
AND n_name = 'CANADA'
ORDER BY s_name;




SELECT
    --Query21
    s.s_name,
    COUNT(*) AS numwait
FROM
    `{schema}.supplier` s
JOIN `{schema}.lineitem` l1 ON s.s_suppkey = l1.l_suppkey
JOIN `{schema}.orders` o ON o.o_orderkey = l1.l_orderkey
JOIN `{schema}.nation` n ON s.s_nationkey = n.n_nationkey
WHERE
    o.o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND l1.l_orderkey IN (
        SELECT l_orderkey
        FROM `{schema}.lineitem`
        GROUP BY l_orderkey
        HAVING COUNT(l_suppkey) > 1
    )
    AND l1.l_orderkey NOT IN (
        SELECT l_orderkey
        FROM `{schema}.lineitem`
        WHERE l_receiptdate > l_commitdate
        GROUP BY l_orderkey
        HAVING COUNT(l_suppkey) > 1
    )
    AND n.n_name = 'SAUDI ARABIA'
GROUP BY s.s_name
ORDER BY numwait DESC, s.s_name
LIMIT 100;





--Query22
--SUBSTRING(x, 1, 2) rather than the ANSI SUBSTRING(x FROM 1 FOR 2): StarRocks rejects
--the FROM/FOR form (candidate run 36214753264), and every bench engine takes the comma form.
WITH avg_acctbal AS (
    SELECT AVG(c_acctbal) AS avg_bal
    FROM `{schema}.customer`
    WHERE c_acctbal > 0
      AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
),
customers_with_orders AS (
    SELECT DISTINCT o_custkey
    FROM `{schema}.orders`
)
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone, 1, 2) AS cntrycode,
        c_acctbal
    FROM `{schema}.customer`
    CROSS JOIN avg_acctbal
    WHERE SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
      AND c_acctbal > avg_bal
      AND c_custkey NOT IN (
          SELECT o_custkey FROM customers_with_orders
      )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;
