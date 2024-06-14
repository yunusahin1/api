from datetime import datetime
current_date = datetime.now().strftime('%Y-%m-%d')
end_date = datetime(datetime.now().year, 12, 31).strftime('%Y-%m-%d')

hotel_id = 313
company_id = 998
room_type_id = 1478
#COMMENT NEW
queries = {
    "hotel_booking": f"""
                        WITH max_created_dates AS (
    SELECT DISTINCT
        h.id AS hotel_id, 
        hb.company_id, 
        hb.considered_date, 
        MAX(hb.created_date) OVER (PARTITION BY hb.company_id, hb.considered_date) AS max_created
    FROM hotel_booking_room_analysis hb
    INNER JOIN hotel h ON hb.company_id = h.company_id 
    WHERE hb.hotel_booking_room_type_id IS null
    and EXTRACT(Year from hb.considered_date) IN (EXTRACT(Year from current_date), EXTRACT(Year from current_date) - 1)
    AND h.id = {hotel_id}
),
base_prices AS (
    SELECT Distinct
        hbbp.base_date, 
        hbbp.base_price, 
        hbbp.min_price, 
        hbbp.max_price 
    FROM hotel_booking_base_price hbbp 
    INNER JOIN hotel_booking_room_type hbrt ON hbbp.room_type_id = hbrt.id 
    WHERE hbrt.hotel_id = {hotel_id}
    AND hbrt.category = 'master_room'
),
hse_tbl as(
	select max(dd.date_actual) as date_actual, max(hse.power) as power from dim_date dd, hotel_booking_holiday_special_event hse
	where dd.date_actual BETWEEN hse.start_date  and hse.end_date 
	and hse.hotel_id =  {hotel_id}
	and hse.is_imported = true
	and EXTRACT(Year from dd.date_actual) IN (EXTRACT(Year from current_date))
)
SELECT 
    hb.considered_date, 
    hb.net_room_revenue, 
    hb.no_rooms, 
    hb.inventory_rooms, 
    hb.cf_occ / 100 as cf_occ, 
    hb.cf_adr_by_room,
    cast(round(CASE 
        WHEN hb.inventory_rooms = 0 THEN hb.net_room_revenue 
        when hb.net_room_revenue = 0 then 0
        ELSE hb.net_room_revenue / hb.inventory_rooms
    end)as numeric) AS revpar,
    hb.considered_date - current_date as days_prior,
    bp.base_price, 
    bp.min_price, 
    bp.max_price, 
    htbl.power AS holiday_power
FROM hotel_booking_room_analysis hb
INNER JOIN max_created_dates mcd ON hb.considered_date = mcd.considered_date 
    AND hb.created_date = mcd.max_created 
    AND hb.company_id = mcd.company_id   
LEFT JOIN base_prices bp ON hb.considered_date = bp.base_date    
LEFT JOIN hse_tbl htbl ON hb.considered_date = htbl.date_actual
WHERE hb.hotel_booking_room_type_id IS NULL 
ORDER BY hb.considered_date asc      
                       """,
    "hotel_booking_2" : f"""
    Select dd.date_actual as considered_date,
cast(count(*)AS DOUBLE PRECISION) / (dty.capacity) as cf_occ,
count(*) as no_rooms,
round(avg(case
                             when hbg.night > 0 then (hbp.total_price/(hb.room_count * hbg.night))
                             else hbp.total_price / hb.room_count
            end)) cf_adr_by_room,
round(sum(hbp.total_price/(hb.room_count * hbg.night))) as net_room_revenue,
round(case
when count(*) > 0 and max(dd.date_actual) > CURRENT_DATE then round(sum(case
when hbg.night > 0 then hbp.total_price / (hb.room_count * hbg.night)
else hbp.total_price / hb.room_count
end)) / (dty.capacity * count(distinct dd.date_actual))
when count(*) > 0 and max(dd.date_actual) <= CURRENT_DATE then round(sum(case
when hbg.night > 0 then hbp.total_price / (hb.room_count * hbg.night)
else hbp.total_price / hb.room_count
end)) / (dty.capacity)
else 1
end) as revpar,
dty.capacity as inventory_rooms
From dim_date dd, hotel_booking_guest hbg, hotel_booking hb
inner join hotel_booking_price hbp on hb.id = hbp.hotel_booking_id
         inner join hotel_booking_room_facility hbrf on hb.id = hbrf.hotel_booking_id
         inner join hotel_booking_and_room_type hbrat on hb.id = hbrat.hotel_booking_id
         inner join hotel_booking_room_type hbrt on hbrat.hotel_booking_room_type_id = hbrt.id,
(SELECT sum(hbrtt.capacity) as capacity
              from hotel_booking_room_type hbrtt
              Where hbrtt.hotel_id = {hotel_id}
              and 1 = 1
              and hbrtt.category is distinct from 'no_show')dty
         where dd.date_actual BETWEEN hb.arrival_date and hb.departure_date
                and (dd.date_actual < hb.departure_date or hb.arrival_date = hb.departure_date)
             and 1 = 1
             and hbg.hotel_booking_id = hb.id
             and (hb.status <> 'CANCELED' or hb.status is null)
          and hb.hotel_id = {hotel_id}
          and hbrf.special_offer != 'Complimentary'
          and hbrt.category is distinct from 'no_show'
           group by dd.date_actual,dty.capacity
           order by dd.date_actual
    
    """  
    ,
    "dp": f"""
    WITH max_created_dates AS (SELECT distinct hdp.hotel_id,
                                                                       hdp.date_day,
                                                                       hdp.room_type_id,
                                                                       MAX(hdp.created_date)
                                                                       OVER (PARTITION BY hdp.hotel_id, hdp.date_day, hdp.room_type_id) AS max_created
                                                       FROM hotel_dynamic_price hdp
                                                       WHERE hdp.hotel_id = {hotel_id}
                                                         AND hdp.room_type_id = {room_type_id}
                                                         AND hdp.date_day BETWEEN '{current_date}' AND '{end_date}'
                                                         and hdp."version" is not null
                                                         and hdp.price_index is not null
                                                       order by date_day asc),
                                 hse_tbl as (select distinct dd.date_actual, hse.power, hc.coefficient
                                             from dim_date dd,
                                                  hotel_booking_holiday_special_event hse,
                                                  hse_coefficient hc
                                             where dd.date_actual BETWEEN hse.start_date and hse.end_date
                                               and hc.power = hse.power
                                               and hse.hotel_id = {hotel_id}
                                               and hse.is_imported = true
                                               and EXTRACT(Year from dd.date_actual) IN (EXTRACT(Year from current_date))
                                               and dd.date_actual BETWEEN '{current_date}' AND '{end_date}')
                            SELECT DISTINCT hdp.room_type_id as room_type,
                                            hdp.date_day,
                                            hdp.previous_week_occupancy,
                                            hdp.current_occupancy,
                                            htbl.power as hse,
                                            hdp.adr,
                                            hdp.price_index,
                                            ROUND(
                                                    CASE
                                                        WHEN (((bp.base_price * 0.4) + (bp.max_price * 0.5) + (bp.min_price * 0.1)) *
                                                              (hdp.price_index)) * COALESCE(htbl.coefficient, 1) < bp.min_price THEN bp.min_price
                                                        WHEN (((bp.base_price * 0.4) + (bp.max_price * 0.5) + (bp.min_price * 0.1)) *
                                                              (hdp.price_index)) * COALESCE(htbl.coefficient, 1) > bp.max_price THEN bp.max_price
                                                        ELSE (((bp.base_price * 0.4) + (bp.max_price * 0.5) + (bp.min_price * 0.1)) *
                                                              (hdp.price_index)) * COALESCE(htbl.coefficient, 1)
                                                        END
                                            )                AS dynamic_price
                            FROM hotel_dynamic_price hdp
                                     INNER JOIN max_created_dates mcd ON hdp.date_day = mcd.date_day AND hdp.created_date = mcd.max_created
                                AND hdp.room_type_id = mcd.room_type_id AND hdp.hotel_id = mcd.hotel_id
                                     inner join hotel_booking_room_type hbrt on hdp.room_type_id = hbrt.id
                                     inner join hotel_booking_base_price bp on hdp.room_type_id = bp.room_type_id and hdp.date_day = bp.base_date
                                     LEFT JOIN hse_tbl htbl ON hdp.date_day = htbl.date_actual
                            ORDER BY hdp.date_day ASC
    """
    ,
    "occupancy_last_week" : f"""
    WITH max_created_dates AS
(SELECT Distinct hb.company_id,
hb.considered_date,
MAX(CASE
WHEN date(hb.created_date) <= CURRENT_DATE and
hb.hotel_booking_room_type_id is null THEN hb.created_date END)
OVER (PARTITION BY hb.company_id, hb.considered_date) AS max_created_current,
MAX(CASE
WHEN date(hb.created_date) <= CURRENT_DATE - 8 and
hb.hotel_booking_room_type_id is null THEN hb.created_date END)
OVER (PARTITION BY hb.company_id, hb.considered_date) AS max_created_previous
FROM hotel_booking_room_analysis hb
Where hb.company_id = {company_id}
and hb.considered_date >='{current_date}')
select
bp.base_date as date_day,
occ_current_dty.occupancy_rates ,
occ_past_dty.occupancy_rates_past
from
hotel_booking_base_price bp
left join
(SELECT mcd.considered_date as date_day,
max(hb.cf_occ) / 100 as occupancy_rates
FROM hotel_booking_room_analysis hb
INNER JOIN
max_created_dates mcd
ON hb.considered_date = mcd.considered_date AND hb.created_date = mcd.max_created_current
Where hb.company_id = mcd.company_id
and hb.hotel_booking_room_type_id is null
GROUP BY mcd.considered_date
ORDER BY mcd.considered_date asc) occ_current_dty
on bp.base_date = occ_current_dty.date_day
left join
(SELECT mcd.considered_date as date_day,
max(hb.cf_occ) / 100 as occupancy_rates_past
FROM hotel_booking_room_analysis hb
INNER JOIN
max_created_dates mcd
ON hb.considered_date = mcd.considered_date AND hb.created_date = mcd.max_created_previous
Where hb.company_id = mcd.company_id
and hb.hotel_booking_room_type_id is null
GROUP BY mcd.considered_date
ORDER BY mcd.considered_date asc) occ_past_dty
on bp.base_date = occ_past_dty.date_day
where bp.room_type_id = {room_type_id}
and bp.base_date >= '{current_date}'
group by
bp.base_date,
occ_current_dty.occupancy_rates,
occ_past_dty.occupancy_rates_past
                          """
    ,
    "max_version": "SELECT MAX(version) as max_version FROM hotel_dynamic_price;"
}
