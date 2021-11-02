DROP FUNCTION IF EXISTS odoo14.corrected_aging_payable_summary(
  CHARACTER VARYING, 
  INTEGER,
  DATE, 
  CHARACTER VARYING
);

CREATE OR REPLACE FUNCTION odoo14.corrected_aging_payable_summary(
  IN p_schema CHARACTER VARYING,
  IN p_interval INTEGER,
  IN p_date DATE,
  IN p_partner_ids CHARACTER VARYING
)
RETURNS TABLE (
  partner_id INT, 
  partner_name CHARACTER VARYING, 
  intv0 NUMERIC,
  intv1 NUMERIC, 
  intv2 NUMERIC, 
  intv3 NUMERIC, 
  intv4 NUMERIC, 
  intv5 NUMERIC, 
  balance NUMERIC
) AS $BODY$

DECLARE
  v_query_string TEXT;
  v_schema_list CHARACTER VARYING [];
  v_schema_name CHARACTER VARYING;

BEGIN

  p_partner_ids := odoo14.ptrim(p_partner_ids);

  SELECT string_to_array(p_schema, ',') INTO v_schema_list;
  FOREACH v_schema_name IN ARRAY v_schema_list LOOP
    SELECT trim(both ' ' FROM v_schema_name) INTO v_schema_name;

     v_query_string = $$
        SELECT partner_id, 
               partner_name, 
               sum(intv0) "intv0", 
               sum(intv1) "intv1", 
               sum(intv2) "intv2", 
               sum(intv3) "intv3", 
               sum(intv4) "intv4", 
               sum(intv5) "intv5", 
               sum(balance) "balance"
        FROM odoo14.corrected_aging_payable_detailed(
           '$$|| v_schema_name ||$$', 
           $$|| p_interval ||$$, 
           '$$|| p_date ||$$', 
           '$$|| p_partner_ids ||$$'
        )
       group by partner_id, partner_name
       order by partner_name


     $$;
     RETURN QUERY EXECUTE v_query_string;
   END LOOP;
END
$BODY$
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION odoo14.corrected_aging_payable_summary(CHARACTER VARYING, INTEGER, DATE, CHARACTER VARYING) IS $$
  This function provides summarized aging receivable with partner filtering and feature to change date interval.

  SIGNATURE
  ---------
  odoo14.corrected_aging_payable_summary(
    IN p_schema CHARACTER VARYING,
    IN p_interval INTEGER,
    IN p_date DATE,
    IN p_partner_ids CHARACTER VARYING
  )

  EXAMPLE
  -------
  1. SELECT partner_id, partner_name, intv0, intv1, intv2, intv3, intv4, intv5, balance
       FROM odoo14.corrected_aging_payable_summary('fdw_sagedistribution_14', 30, now()::date, '(0)');

  2. SELECT partner_id, partner_name, intv0, intv1, intv2, intv3, intv4, intv5, balance
       FROM odoo14.corrected_aging_payable_summary('fdw_powerfuels', 30, '2019-10-31', '(2850, 2582, 2429, 2759, 2577, 2859, 2574, 2545, 2776, 2414)');

$$;
