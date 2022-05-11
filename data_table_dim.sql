truncate dim_province RESTART identity CASCADE;
truncate dim_district RESTART identity cascade;
truncate dim_case RESTART identity cascade;

INSERT INTO dim_case (status,status_detail)
select status,status_detail from temp_tabel_case;

INSERT INTO dim_province (province_id,province_name)
select kode_prov,nama_prov from raw_data_table
on conflict do nothing;

INSERT INTO dim_district (district_id,province_id,district_name)
select kode_kab,kode_prov,nama_kab from raw_data_table
on conflict do nothing;

CREATE TABLE IF NOT EXISTS temp_fact_table
as 
select kode_prov, kode_kab, tanggal,
unnest(array['suspect_diisolasi','suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi','probable_discarded','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']) as detail_name,
unnest(array[suspect_diisolasi,suspect_discarded, closecontact_dikarantina, closecontact_discarded, probable_diisolasi,probable_discarded,confirmation_sembuh,confirmation_meninggal,suspect_meninggal,closecontact_meninggal,probable_meninggal]) as value
from raw_data_table;