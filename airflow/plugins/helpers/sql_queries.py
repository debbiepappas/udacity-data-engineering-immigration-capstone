class SqlQueries:
    
    # create fact and dimension insert queries
    pleasurevisits_table_insert = ("""
        INSERT INTO public.pleasurevisits
        SELECT
                md5(immigr.adm_num || immigr.arrival_date) pleasurevisit_id,
                immigr.arrival_date,
                immigr.dep_date,
                immigr.adm_num,
                immigr.flight_num,
                demo.city,
                immigr.us_port,
                immigr.visatype      
        FROM staging_immigr immigr
        LEFT JOIN staging_demo demo
            ON immigr.us_state = demo.state
                AND immigr.age = demo.median_age
        WHERE immigr.visa_code = 'Pleasure'        
    """)

    flights_table_insert = ("""
        TRUNCATE TABLE public.flights ;
        INSERT INTO public.flights
        SELECT distinct flight_num, airline, us_port, us_state, year
        FROM staging_immigr
        WHERE visa_code = 'Pleasure'
    """)

    visitors_table_insert = ("""
        TRUNCATE TABLE public.visitors ;
        INSERT INTO public.visitors
        SELECT distinct adm_num, country, age, gender, visatype
        FROM staging_immigr
        WHERE visa_code = 'Pleasure'
    """)

    cities_table_insert = ("""
        TRUNCATE TABLE public.cities ;
        INSERT INTO public.cities
        SELECT distinct city, state, median_age, house_size, male_pop, female_pop, total_pop
        FROM staging_demo
    """)

    arrival_table_insert = ("""
        TRUNCATE TABLE public.arrival ;
        INSERT INTO public.arrival
        SELECT arrival_date, extract(day from arrival_date), extract(week from arrival_date), 
               extract(month from arrival_date), extract(year from arrival_date), extract(dayofweek from arrival_date)
        FROM pleasurevisits
    """)
    
    departure_table_insert = ("""
        TRUNCATE TABLE public.departure ;
        INSERT INTO public.departure
        SELECT dep_date, extract(day from dep_date), extract(week from dep_date), 
               extract(month from dep_date), extract(year from dep_date), extract(dayofweek from dep_date)
        FROM pleasurevisits
    """)
    
    # Create staging, fact and dimension tables
    
    staging_immigr_table_create = ("""
        DROP TABLE IF EXISTS public.staging_immigr;
        CREATE TABLE IF NOT EXISTS public.staging_immigr (
	        cic_id int4,
            year int4,
            month int4,
            country int4,
            us_port varchar(256),
            arrival_date date,
            travel_mode varchar(256),
            us_state varchar(256),
            dep_date date,
            age int4,
            visa_code varchar(256),
            gender varchar(256),
            airline varchar(256),
            adm_num int8,
            flight_num varchar(256),
            visatype varchar(256) )
    """)
    
    staging_demo_table_create = ("""
        DROP TABLE IF EXISTS public.staging_demo;
        CREATE TABLE IF NOT EXISTS public.staging_demo (    
            city varchar(256),
            median_age int4,
            male_pop int4,
            female_pop int4,
            total_pop int4,
            veterans int4,
            foreign_born int4,
            house_size int4,
            state varchar(256),
            race varchar(256),
            race_count int4  )
    """)
    
    pleasurevisits_table_create = ("""
        DROP TABLE IF EXISTS public.pleasurevisits;
        CREATE TABLE IF NOT EXISTS pleasurevisits ( 
            pleasurevisit_id varchar(256) NOT NULL,
	        arrival_date date NOT NULL,
	        dep_date date NOT NULL,
	        adm_num int8 NOT NULL,
            flight_num varchar(256),
            city varchar(256),
            us_port varchar(256),
            visatype varchar(256),
	        CONSTRAINT pleasurevisits_pkey PRIMARY KEY (pleasurevisit_id) )    
    """)    
    
    flights_table_create = ("""
        DROP TABLE IF EXISTS public.flights;
        CREATE TABLE IF NOT EXISTS flights (     
            flight_num varchar(256) NOT NULL,
	        airline varchar(256),
            us_port varchar(256),
            us_state varchar(256),
            year int4,
	        CONSTRAINT flights_pkey PRIMARY KEY (flight_num) )
    """)    
    
    cities_table_create = ("""
        DROP TABLE IF EXISTS public.cities;
        CREATE TABLE IF NOT EXISTS cities (      
            city varchar(256) NOT NULL,
	        state varchar(256),
            median_age int4,
            house_size int4,
            male_pop int4,
            female_pop int4,
            total_pop int4,
	        CONSTRAINT cities_pkey PRIMARY KEY (city) )    
    """)
    
    visitors_table_create = ("""
        DROP TABLE IF EXISTS public.visitors;
        CREATE TABLE IF NOT EXISTS visitors (     
            adm_num int8 NOT NULL,
	        country int4,
            age int4,
            gender varchar(256),
            visatype varchar(256),
	        CONSTRAINT visitors_pkey PRIMARY KEY (adm_num) )
    """)
    
    
    arrival_table_create = ("""
        DROP TABLE IF EXISTS public.arrival;
        CREATE TABLE IF NOT EXISTS arrival (        
            arrival_date date NOT NULL,
            day integer,
            week integer,
            month text,
            year integer,
            weekday text,
            CONSTRAINT arrival_pkey PRIMARY KEY (arrival_date) )
    """)
    
    departure_table_create = ("""
        DROP TABLE IF EXISTS public.departure;
        CREATE TABLE IF NOT EXISTS departure (        
            dep_date date NOT NULL,
            day integer,
            week integer,
            month text,
            year integer,
            weekday text,
            CONSTRAINT departure_pkey PRIMARY KEY (dep_date) )
    """)   
    
    