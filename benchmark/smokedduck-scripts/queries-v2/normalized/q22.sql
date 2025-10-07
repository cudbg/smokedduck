SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal,
FROM (
    SELECT
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal,
    FROM
        customer, (
            SELECT
                avg(c_acctbal) as v
            FROM
                customer
            WHERE
                c_acctbal > 0.00
                AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')) as t1
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > t1.v 
        AND NOT EXISTS (
                SELECT
                    *
                FROM
                    orders
                WHERE
                    o_custkey = c_custkey)) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode
