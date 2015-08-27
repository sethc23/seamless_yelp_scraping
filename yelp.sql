DROP TABLE IF EXISTS yelp;
CREATE TABLE yelp (
    id text,
    vend_name text,
    phone bigint,
    address text,
    city text,
    state_code text,
    postal_code bigint,
    display_phone text,
    is_claimed boolean,
    is_closed boolean,
    menu_date_updated text,
    menu_provider text,
    rating double precision,
    review_count bigint DEFAULT 0,
    categories text,
    url text,
    latitude double precision,
    longitude double precision,
    geo_accuracy double precision,
    hours text,
    hours_updated timestamp with time zone,
    extra_info text,
    menu_page text,
    website text,
    online_ordering boolean DEFAULT false,
    price_range text,
    geom geometry(Point,4326),
    bbl integer,
    camis integer,
    lot_cnt integer DEFAULT 1,
    norm_addr text,
    last_api_update timestamp with time zone,
    neighborhoods text,
    display_address text,
    upd_search_links timestamp with time zone,
    phone_as_text text,
    non_mn_zipcode boolean DEFAULT false,
    last_updated timestamp with time zone,
    vend_cnt integer,
    uid integer NOT NULL DEFAULT z_next_free('yelp'::text, 'uid'::text, 'yelp_uid_seq'::text),
    recent_deliv_cmts boolean DEFAULT false,
    tmp boolean DEFAULT false,
    sl_vend_id bigint,
    checked_out text,
    bldg text,
    box text,
    unit text,
    num text,
    predir text,
    street_name text,
    suftype text,
    sufdir text,
    bldg_street_idx text,
    gc_lat double precision,
    gc_lon double precision,
    gc_full_addr text,
    tmp_i integer,
    gc_zip bigint,
    gc_addr text,
    trigger_step text,
    orig_addr text,
    CONSTRAINT yelp_pkey PRIMARY KEY (uid)
)