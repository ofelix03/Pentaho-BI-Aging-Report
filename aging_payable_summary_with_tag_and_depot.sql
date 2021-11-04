DROP FUNCTION IF EXISTS odoo14.corrected_aging_payable_summary_with_tag_and_depot(
	CHARACTER VARYING, 
	INTEGER, DATE,
	CHARACTER VARYING, 
	CHARACTER VARYING, 
	CHARACTER VARYING, 
	CHARACTER VARYING, 
	INTEGER
);

CREATE OR REPLACE FUNCTION odoo14.corrected_aging_payable_summary_with_tag_and_depot(
  IN p_schema CHARACTER VARYING,
  IN p_interval INTEGER,
  IN p_date DATE,
  IN p_partner_ids CHARACTER VARYING,
  IN p_partner_tag_ids CHARACTER VARYING,
  IN p_account_ids CHARACTER VARYING,
  IN p_depot_ids character varying,
  IN p_currency_id integer
)

RETURNS TABLE (
  partner_id integer, 
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

  SELECT string_to_array(p_schema, ',') INTO v_schema_list;
  FOREACH v_schema_name IN ARRAY v_schema_list LOOP
    SELECT trim(both ' ' FROM v_schema_name) INTO v_schema_name;
  
    v_query_string = $$
        select partner_id,
                upper(partner_name)::CHARACTER VARYING "partner_name",
                sum(intv0) "intv0",
                sum(intv1) "intv1",
                sum(intv2) "intv2",
                sum(intv3) "intv3",
                sum(intv4) "intv4",
                sum(intv5) "intv5",
                sum(balance) "balance"
        from odoo14.corrected_aging_payable_with_tag_and_depot(
                '$$|| v_schema_name ||$$'::CHARACTER VARYING,
                 $$|| p_interval ||$$,
                '$$|| p_date ||$$'::DATE,
                '$$|| p_partner_ids ||$$'::CHARACTER VARYING,
                '$$|| p_partner_tag_ids ||$$'::CHARACTER VARYING,
                '$$|| p_account_ids ||$$'::CHARACTER VARYING,
                '$$|| p_depot_ids ||$$'::CHARACTER VARYING,
                $$|| p_currency_id ||$$
        ) tbl group by partner_id, partner_name
        order by partner_name
    
    $$;
    RETURN QUERY EXECUTE v_query_string;

  END LOOP;
END
$BODY$
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION odoo14.corrected_aging_payable_summary_with_tag_and_depot(CHARACTER VARYING, INTEGER, DATE, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, character varying, integer) IS $$
  This function provides summarized aging receivable with partner filtering and feature to change date interval.

  SIGNATURE
  ---------
  odoo14.corrected_aging_payable_summary_with_tag_and_depot(
    IN p_schema CHARACTER VARYING,
    IN p_interval INTEGER,
    IN p_date DATE,
	in p_partner_ids character varying,
    IN p_partner_tag_ids CHARACTER VARYING
    IN p_account_ids CHARACTER VARYING,
	IN p_depot_ids character varying,
	IN p_currency_id integer
  )

  EXAMPLE
  -------
  1. SELECT  partner_name, intv0, intv1, intv2, intv3, intv4, intv5, balance
     FROM odoo14.corrected_aging_payable_summary_with_tag_and_depot('fdw_sagedistribution_14', 30, now()::DATE, '(0)', '(0)', '(0)', '(0)', 0)
	 
$$;






