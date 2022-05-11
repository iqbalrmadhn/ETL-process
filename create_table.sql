drop table if exists temp_fact_table;

CREATE TABLE IF NOT EXISTS dim_case (
    id SERIAL PRIMARY KEY,
    status varchar(20),
    status_detail varchar(20)
	);


CREATE TABLE IF NOT EXISTS dim_province (
    province_id INT PRIMARY KEY,
	province_name VARCHAR(16)
	);

CREATE TABLE IF NOT EXISTS dim_district (
    district_id INT PRIMARY KEY,
    province_id INT,
	district_name VARCHAR(32)
	);


CREATE TABLE IF NOT EXISTS fact_province_daily (
    id serial PRIMARY KEY,
	province_id INT,
	case_id INT,
	tanggal VARCHAR(20),
	total INT,
	FOREIGN KEY (province_id) REFERENCES dim_province (province_id),
	FOREIGN KEY (case_id) REFERENCES dim_case (id)
	);

CREATE TABLE IF NOT EXISTS fact_province_monthly (
    id serial PRIMARY KEY,
	province_id INT,
	case_id INT,
	bulan VARCHAR(10),
	total INT,
	FOREIGN KEY (province_id) REFERENCES dim_province (province_id),
	FOREIGN KEY (case_id) REFERENCES dim_case (id)
	);

CREATE TABLE IF NOT EXISTS fact_province_yearly (
    id serial PRIMARY KEY,
	province_id INT,
	case_id INT,
	tahun VARCHAR(4),
	total INT,
	FOREIGN KEY (province_id) REFERENCES dim_province (province_id),
	FOREIGN KEY (case_id) REFERENCES dim_case (id)
	);

CREATE TABLE IF NOT EXISTS fact_district_monthly (
    id serial PRIMARY KEY,
	district_id INT,
	case_id INT,
	bulan VARCHAR(10),
	total INT,
	FOREIGN KEY (district_id) REFERENCES dim_district (district_id),
	FOREIGN KEY (case_id) REFERENCES dim_case (id)
	);

CREATE TABLE IF NOT EXISTS fact_district_yearly (
    id serial PRIMARY KEY,
	district_id INT,
	case_id INT,
	tahun VARCHAR(4),
	total INT,
	FOREIGN KEY (district_id) REFERENCES dim_district (district_id),
	FOREIGN KEY (case_id) REFERENCES dim_case (id)
	);
