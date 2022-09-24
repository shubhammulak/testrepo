/* NVR version */
select
	snapshot_date,
	opportunity_name_h,
	opportunity_number,
	opportunity_split_owner_full_name,
	oso_current,
	rep_region,
	asa,
	sa_manager,
	sa_manager_region,
	csa,
	opportunity_forecast_category,
	opportunity_close_date,
	close_quarter,
	close_year,
	close_year + '-' + close_quarter as real_fiscal_year_and_quarter,
	c.fiscal_year_quarter_name,
	/* not sure */
	opportunity_channel,
	account_name,
	opportunity_touched_flag,
	reporting_region,
	account_id, 
	product_group_detail,
	opportunity_ssi_in_scope_flag,
	account_segment,
	account_subsegment,
	opportunity_stage_name,
	SUM(actual_syb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as syb,
	SUM(actual_tb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as tb,
	sum(renewal_available_syb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as renewal_available_syb,
	sum(renewal_available_tb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as renewal_available_tb,
	sum(renewal_at_par_syb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as renewal_at_par_syb,
	sum(renewal_at_par_tb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as renewal_at_par_tb,
	SUM(actual_syb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd))-sum(renewal_at_par_syb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as new_business_syb,
	SUM(actual_tb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd))-sum(renewal_at_par_tb_amount_usd_cy_pr * (1 / aud.current_year_plan_rate_usd)) as new_business_tb,
	forecast_week_number,
	c.day_of_week_number,
	c.TW_LW
from
	(
	select
		*
	from
		(
		select
			*,
			left(opportunity_close_date, 4) as close_year,
			concat('Q',(select date_part(quarter, opportunity_close_date))) as close_quarter
		from
			rsds_ops_apac.core_nvr_pipeline_snapshot_daily_cq a
		where
			snapshot_date between /* Ram to update so it takes 1st day of quarter we are currently in */
			case
				when date_part(quarter , current_date) = 1 then 
			cast(date_part(year , current_date) as varchar)+ '-01-01'
				when date_part(quarter , current_date) = 2 then 
			cast(date_part(year , current_date) as varchar)+ '-04-01'
				when date_part(quarter , current_date) = 3 then 
			cast(date_part(year , current_date) as varchar)+ '-07-01'
				when date_part(quarter , current_date) = 4 then 
			cast(date_part(year , current_date) as varchar)+ '-10-01'
			end
			and (
			select
				max(snapshot_date)
			from
				rsds_ops_apac.core_nvr_pipeline_snapshot_daily_cq)
			and reporting_region = 'ANZ'
			and 
			case
				when date_part(quarter , current_date) = 1 then 
			("opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date) as varchar)
					or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date)+ 1 as varchar)
						or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date)+ 2 as varchar)
							or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date)+ 3 as varchar))
				when date_part(quarter , current_date) = 2 then 
			("opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date) as varchar)
					or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date)+ 1 as varchar)
						or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date)+ 2 as varchar)
							or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date)+ 1 as varchar)+ '-Q' + cast(date_part(quarter , current_date)-1 as varchar))
				when date_part(quarter , current_date) = 3 then 
			("opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date) as varchar)
					or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date)+ 1 as varchar)
						or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date)+ 1 as varchar)+ '-Q' + cast(date_part(quarter , current_date)-2 as varchar)
							or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date)+ 1 as varchar)+ '-Q' + cast(date_part(quarter , current_date)-1 as varchar))
				when date_part(quarter , current_date) = 4 then 
			("opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date) as varchar)+ '-Q' + cast(date_part(quarter , current_date) as varchar)
					or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date)+ 1 as varchar)+ '-Q' + cast(date_part(quarter , current_date)-3 as varchar)
						or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date)+ 1 as varchar)+ '-Q' + cast(date_part(quarter , current_date)-2 as varchar)
							or "opportunity_close_date_fiscal_year_quarter_q" = cast(date_part(year , current_date)+ 1 as varchar)+ '-Q' + cast(date_part(quarter , current_date)-1 as varchar))
			end
			 /*Ram to change this to look at CQ + CQ + 3 */
			and opportunity_name_h not like '%Opp-NFR%'
			and opportunity_type_h not in ('Bridge', 'Buffer', 'Customer Satisfaction', 'Master Claim')
			and opportunity_split_type_h = 'Bookings'
			and product_line not like '%Travel and Expenses%'
			and opportunity_name_h not like '%Opp-NFR%' )
	left join rsds_ops_apac.anz_cy22_rep_mapping b on
		opportunity_split_owner_full_name = b.account_owner
		and opportunity_close_date_fiscal_quarter_name = b.quarter
		and opportunity_close_date_fiscal_year_number = b."year" )
left join rsds_ops_planning.base_currency aud on aud.currency_code = 'AUD'			
inner join(with snap_table as (
select
		s.*,
		e.day_of_week_number,
		e.forecast_week_number,
		e.calendar_date,
		e.fiscal_year_quarter_name
from
		(
	select
			cast(max(snapshot_date) as date) as snap_date
	from
			rsds_ops_apac.core_nvr_pipeline_snapshot_daily_cq) s
left join rsds_ops_planning.base_calendar_v2 e on
		s.snap_date = e.calendar_date)
		
select
	snap_date as calendar_date,
	day_of_week_number,
	forecast_week_number,
	fiscal_year_quarter_name,
	'TW' as TW_LW 
	
from
	snap_table
union all
select
	calendar_date,
	day_of_week_number,
	forecast_week_number,
	fiscal_year_quarter_name,
	'LW' as TW_LW 
from
	rsds_ops_planning.base_calendar_v2
where
	forecast_week_number = (
	select
		forecast_week_number-1
	from
		snap_table)
	and day_of_week_number = 6
	and fiscal_year_quarter_name = (
	select
		fiscal_year_quarter_name
	from
		snap_table)
union all
select
	calendar_date,
	day_of_week_number,
	forecast_week_number,
	fiscal_year_quarter_name,
	'W1' as TW_LW 
from
	rsds_ops_planning.base_calendar_v2
where
	forecast_week_number = 1
	and day_of_week_number = 6
	and fiscal_year_quarter_name = (
	select
		fiscal_year_quarter_name
	from
		snap_table)
union all
select
	calendar_date,
	day_of_week_number,
	forecast_week_number,
	fiscal_year_quarter_name,
	'W3' as TW_LW 
from
	rsds_ops_planning.base_calendar_v2
where
	forecast_week_number = 3
	and day_of_week_number = 6
	and fiscal_year_quarter_name = (
	select
		fiscal_year_quarter_name
	from
		snap_table)
union all
select
	calendar_date,
	day_of_week_number,
	forecast_week_number,
	fiscal_year_quarter_name,
	'W6' as TW_LW 
from
	rsds_ops_planning.base_calendar_v2
where
	forecast_week_number = 6
	and day_of_week_number = 6
	and fiscal_year_quarter_name = (
	select
		fiscal_year_quarter_name
	from
		snap_table)) c on 
		snapshot_date = c.calendar_date
group by
	snapshot_date,
	opportunity_name_h,
	opportunity_number,
	opportunity_split_owner_full_name,
	oso_current,
	rep_region,
	asa,
	sa_manager,
	sa_manager_region,
	csa,
	opportunity_forecast_category,
	opportunity_close_date,
	close_quarter,
	close_year,
	real_fiscal_year_and_quarter,
	c.fiscal_year_quarter_name,
	opportunity_channel,
	account_name,
	opportunity_touched_flag,
	reporting_region,
	account_id,
	product_group_detail,
	opportunity_ssi_in_scope_flag,
	opportunity_stage_name,
	account_segment,
	account_subsegment,
	forecast_week_number,
	c.day_of_week_number,
	c.TW_LW