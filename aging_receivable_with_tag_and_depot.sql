DROP FUNCTION IF EXISTS odoo14.corrected_aging_receivable_with_tag_and_depot(
	CHARACTER VARYING, 
	INTEGER, DATE,
	CHARACTER VARYING, 
	CHARACTER VARYING, 
	CHARACTER VARYING, 
	CHARACTER VARYING, 
	INTEGER
);

CREATE OR REPLACE FUNCTION odoo14.corrected_aging_receivable_with_tag_and_depot(
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
  days INTEGER, 
  "date" DATE, 
  "ref" CHARACTER VARYING,
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

  p_partner_tag_ids := odoo14.ptrim(p_partner_tag_ids);
  p_account_ids := odoo14.ptrim(p_account_ids);


  SELECT string_to_array(p_schema, ',') INTO v_schema_list;
  FOREACH v_schema_name IN ARRAY v_schema_list LOOP
    SELECT trim(both ' ' FROM v_schema_name) INTO v_schema_name;
  
    v_query_string = $$
        with q as (
            select aml.partner_id, 
                aml.date "aging_date", 
                ($$|| quote_literal(p_date) ||$$::DATE - aml.date) "days", 
                (cup.payment_residual * -1) "residual", 
                aml.ref
            from (
                select payment_line_id, 	   
                    sum(amount) "payment_residual"
                from (
                    select aml.id "payment_line_id", 
                        sum(aml.credit) "amount"
                    from $$|| v_schema_name ||$$.account_move_line aml
                    left join $$|| v_schema_name ||$$.account_account aa
                        on aa.id = aml.account_id
                    where aa.internal_type = 'receivable'
                    and aml.debit = 0
                    AND CASE WHEN '0' = '$$|| p_currency_id ||$$' THEN 
                        TRUE
                    WHEN (
                        SELECT sum(id) FROM (
                            SELECT id FROM $$|| quote_ident(v_schema_name) ||$$.res_company
                            WHERE currency_id = $$|| p_currency_id ||$$
                            UNION ALL SELECT 0 id
                        ) tbl
                    )  != 0 THEN
                        (aml.currency_id IS NULL OR aml.currency_id = $$|| p_currency_id ||$$)
                        ELSE aml.currency_id = $$|| p_currency_id ||$$
                    END
                    group by aml.id

                    union all

                    select apr.credit_move_id "payment_line_id", 
                        (sum(amount) * -1) "amount"
                    from $$|| v_schema_name ||$$.account_partial_reconcile apr
                    left join $$|| v_schema_name ||$$.account_move_line aml
                        on aml.id = apr.debit_move_id
                    left join $$|| v_schema_name ||$$.account_account aa
                        on aa.id = aml.account_id
                    where aa.internal_type = 'receivable'
                    and aml.credit = 0
                    AND CASE WHEN '0' = '$$|| p_currency_id ||$$' THEN TRUE
                    WHEN (
                        SELECT sum(id) FROM (
                            SELECT id FROM $$|| quote_ident(v_schema_name) ||$$.res_company
                            WHERE currency_id = $$|| p_currency_id ||$$
                            UNION ALL SELECT 0 id
                        ) tbl
                    )  != 0 THEN
                        (apr.credit_currency_id IS NULL OR apr.credit_currency_id = $$|| p_currency_id ||$$)
                        ELSE apr.credit_currency_id = $$|| p_currency_id ||$$
                    END
                    group by apr.credit_move_id
                ) tbl 
                group by payment_line_id	
            ) cup
            left join $$|| v_schema_name ||$$.account_move_line aml
            on aml.id = cup.payment_line_id
            where abs(cup.payment_residual) > 0.01


            union all

            select aml.partner_id, 
                aml.date_maturity "aging_date",
               ($$|| quote_literal(p_date) ||$$::DATE - aml.date_maturity) "days",
                aml.amount_residual,
                am.name "ref"
            from $$|| v_schema_name ||$$.account_move_line aml
            left join $$|| v_schema_name ||$$.account_move am
                on am.id = aml.move_id
            left join $$|| v_schema_name ||$$.account_account aa
                on aa.id = aml.account_id
            left join $$|| v_schema_name ||$$.stock_warehouse sw
                on sw.id = am.warehouse_id
            where am.state = 'posted'
            and aa.internal_type = 'receivable'
            and am.move_type = 'out_invoice'
            and aml.amount_residual > 0
            AND CASE WHEN string_to_array('$$|| odoo14.ptrim(p_depot_ids) ||$$', ',')::integer[] @> string_to_array('0', ',')::integer[] 
                THEN TRUE
            ELSE
                sw.id IN (SELECT unnest(string_to_array('$$|| odoo14.ptrim(p_depot_ids) ||$$', ',')::INTEGER []))
            END 
            AND CASE WHEN '0' = '$$|| p_currency_id ||$$' THEN TRUE
            WHEN (
                SELECT sum(id) FROM (
                    SELECT id FROM $$|| quote_ident(v_schema_name) ||$$.res_company
                    WHERE currency_id = $$|| p_currency_id ||$$
                    UNION ALL SELECT 0 id
                ) tbl
            )  != 0 THEN
                (aml.currency_id IS NULL  OR aml.currency_id = $$|| p_currency_id ||$$)
                ELSE aml.currency_id = $$|| p_currency_id ||$$
            END
            AND CASE WHEN '$$|| odoo14.ptrim(p_account_ids) ||$$' = '0' THEN TRUE 
                ELSE aml.account_id IN (SELECT unnest(string_to_array('$$|| odoo14.ptrim(p_account_ids) ||$$', ',')::INTEGER [])) 
            END
        )
        SELECT q.partner_id, 
                upper(rp.name)::CHARACTER VARYING AS partner_name,
                q.days,
                q.aging_date AS "date", 
                q.ref, 
                CASE WHEN q.days < 0 THEN q.residual ELSE 0 END AS intv0,
                CASE WHEN q.days >= 0 AND q.days <= $$|| p_interval ||$$ THEN q.residual ELSE 0 END AS intv1,
                CASE WHEN q.days > $$|| p_interval ||$$ AND q.days <= ($$|| p_interval ||$$ * 2) THEN q.residual ELSE 0 END AS intv2,
                CASE WHEN q.days > ($$|| p_interval ||$$ * 2) AND q.days <= ($$|| p_interval ||$$ * 3) THEN q.residual ELSE 0 END AS intv3,
                CASE WHEN q.days > ($$|| p_interval ||$$ * 3) AND q.days <= ($$|| p_interval ||$$ * 4) THEN q.residual ELSE 0 END AS intv4,
                CASE WHEN q.days > ($$|| p_interval ||$$ * 4) THEN q.residual ELSE 0 END AS intv5,
                q.residual AS balance
        FROM q
        left join $$|| v_schema_name ||$$.res_partner rp
        on q.partner_id = rp.id
        WHERE q.partner_id = rp.id
            AND rp.is_company IS TRUE
            AND CASE WHEN '$$|| odoo14.ptrim(p_partner_ids) ||$$'='0' THEN 
                TRUE
            ELSE 
                q.partner_id IN (SELECT unnest(string_to_array('$$|| odoo14.ptrim(p_partner_ids) ||$$', ',')::INTEGER [])) 
            END
            AND CASE WHEN '$$|| odoo14.ptrim(p_partner_tag_ids) ||$$'='0' THEN 
                TRUE 
            ELSE 
                EXISTS(
                    SELECT 1 FROM $$|| v_schema_name ||$$.res_partner_res_partner_category_rel rprpc
                    WHERE rprpc.partner_id =  rp.id AND rprpc.category_id in (
                        SELECT unnest(string_to_array('$$|| odoo14.ptrim(p_partner_tag_ids) ||$$', ',')::INTEGER [])
                    )
                )
            END
        ORDER BY rp.name 
    $$;
    RETURN QUERY EXECUTE v_query_string;

  END LOOP;
END
$BODY$
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION odoo14.corrected_aging_receivable_with_tag_and_depot(CHARACTER VARYING, INTEGER, DATE, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, character varying, integer) IS $$
  This function provides summarized aging receivable with partner filtering and feature to change date interval.

  SIGNATURE
  ---------
  odoo14.corrected_aging_receivable_with_tag_and_depot(
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
     FROM odoo14.corrected_aging_receivable_with_tag_and_depot('fdw_sagedistribution_14', 30, now()::DATE, '(0)', '(0)', '(0)', '(0)', 0)
	 
$$;






