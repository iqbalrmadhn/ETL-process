truncate fact_province_daily RESTART identity cascade;
truncate fact_province_monthly RESTART identity cascade;
truncate fact_province_yearly RESTART identity cascade;
truncate fact_district_monthly RESTART identity cascade;
truncate fact_district_yearly RESTART identity cascade;

INSERT INTO fact_province_daily (province_id,case_id,tanggal,total)
select a.province_id, c.id, b.tanggal, sum(b.value) as total
from dim_province a 
join temp_fact_table b on a.province_id = b.kode_prov 
join dim_case c on b.detail_name = (CONCAT(c.status,'_', c.status_detail))
group by a.province_id, b.tanggal, c.id
order by b.tanggal;

INSERT INTO fact_province_monthly (province_id,case_id,bulan,total)
select a.province_id, c.id, to_char(CAST(b.tanggal AS DATE),'YYYY-MM') as bulan,sum(b.value) as total
from dim_province a 
join temp_fact_table b on a.province_id = b.kode_prov 
join dim_case c on b.detail_name = (CONCAT(c.status,'_', c.status_detail))
group by a.province_id, bulan, c.id
order by bulan;

INSERT INTO fact_province_yearly (province_id,case_id,tahun,total)
select a.province_id, c.id, to_char(CAST(b.tanggal AS DATE),'YYYY') as tahun,sum(b.value) as total
from dim_province a 
join temp_fact_table b on a.province_id = b.kode_prov 
join dim_case c on b.detail_name = (CONCAT(c.status,'_', c.status_detail))
group by a.province_id, tahun, c.id
order by tahun;

INSERT INTO fact_district_monthly(district_id,case_id,bulan,total)
select a.district_id, c.id, to_char(CAST(b.tanggal AS DATE),'YYYY-MM') as bulan,sum(b.value) as total
from dim_district a 
join temp_fact_table b on a.district_id = b.kode_kab
join dim_case c on b.detail_name = (CONCAT(c.status,'_', c.status_detail))
group by a.district_id, bulan, c.id
order by bulan;

INSERT INTO fact_district_yearly (district_id,case_id,tahun,total)
select a.district_id, c.id, to_char(CAST(b.tanggal AS DATE),'YYYY') as tahun,sum(b.value) as total
from dim_district a 
join temp_fact_table b on a.district_id = b.kode_kab
join dim_case c on b.detail_name = (CONCAT(c.status,'_', c.status_detail))
group by a.district_id, tahun, c.id
order by tahun;