DROP FUNCTION IF EXISTS odoo14.corrected_aging_payable_detailed(
  CHARACTER VARYING, 
  INTEGER, 
  DATE, 
  CHARACTER VARYING
);

CREATE OR REPLACE FUNCTION odoo14.corrected_aging_payable_detailed(
  IN p_schema CHARACTER VARYING,
  IN p_interval INTEGER,
  IN p_date DATE,
  IN p_partner_ids CHARACTER VARYING
)
RETURNS TABLE (
  partner_id INTEGER, 
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

  SELECT string_to_array(p_schema, ',') INTO v_schema_list;
  FOREACH v_schema_name IN ARRAY v_schema_list LOOP
    SELECT trim(both ' ' FROM v_schema_name) INTO v_schema_name;

    v_query_string:= $$
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
                    -- Registered payments
                    select aml.id "payment_line_id", 
                           aml.debit "amount"
                    from $$|| v_schema_name ||$$.account_move_line aml
                    left join $$|| v_schema_name ||$$.account_account aa
                        on aa.id = aml.account_id
                    where aa.internal_type = 'payable'
                    and aml.debit <> 0

                    union all

                    -- Partial payment
                    select apr.debit_move_id "payment_line_id", 
                        sum(amount) * -1 "amount"
                    from $$|| v_schema_name ||$$.account_partial_reconcile apr
                    left join $$|| v_schema_name ||$$.account_move_line aml
                    on aml.id = apr.debit_move_id
                    left join $$|| v_schema_name ||$$.account_account aa
                    on aa.id = aml.account_id
                    where aa.internal_type = 'payable'
                    group by apr.debit_move_id
                    
                ) tbl 
                group by payment_line_id	
            ) cup
            left join $$|| v_schema_name ||$$.account_move_line aml
            on aml.id = cup.payment_line_id
            left join $$|| v_schema_name ||$$.account_move am
            on am.id = aml.move_id
            where abs(cup.payment_residual) > 0.01
            and am.state = 'posted'

            union all

            select aml.partner_id, 
                aml.date_maturity "aging_date",
               ($$|| quote_literal(p_date) ||$$::DATE - aml.date_maturity) "days",
                abs(aml.amount_residual) "residual",
                am.name "ref"
            from $$|| v_schema_name ||$$.account_move_line aml
            inner join $$|| v_schema_name ||$$.account_move am
                on am.id = aml.move_id
            inner join $$|| v_schema_name ||$$.account_account aa
                on aa.id = aml.account_id
            where am.state = 'posted'
            and aa.internal_type = 'payable'
            and credit <> 0
            and abs(aml.amount_residual) > 0.01
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
            AND CASE WHEN '$$|| odoo14.ptrim(p_partner_ids) ||$$'='0' THEN TRUE
            ELSE q.partner_id IN (SELECT unnest(string_to_array('$$|| odoo14.ptrim(p_partner_ids) ||$$', ',')::INTEGER [])) END
        ORDER BY rp.name, q.aging_date DESC

    $$;
    RETURN QUERY EXECUTE v_query_string;
  END LOOP;
END
$BODY$
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION odoo14.corrected_aging_payable_detailed(CHARACTER VARYING, INTEGER, DATE, CHARACTER VARYING) IS $$
  This function provides detailed aging receivable with partner filtering and feature to change date interval.

  SIGNATURE
  ---------
  odoo14.corrected_aging_payable_detailed(
    IN p_schema CHARACTER VARYING,
    IN p_interval INTEGER,
    IN p_date DATE,
    IN p_partner_ids CHARACTER VARYING
  )

  EXAMPLE
  -------
  1. SELECT partner_id, partner_name, days, "date", "ref", intv0, intv1, intv2, intv3, intv4, intv5, balance
       FROM odoo14.corrected_aging_payable_detailed('fdw_sagedistribution_14', 30, now()::date, '(0)');

  2. SELECT partner_id, partner_name, days, "date", "ref", intv0, intv1, intv2, intv3, intv4, intv5, balance
       FROM odoo14.corrected_aging_payable_detailed('fdw_sagedistribution', 30, '2019-09-03, '(2850, 2582, 2429, 2759, 2577, 2859, 2574, 2545, 2776, 2414)');

$$;