# HubSpot Rental Analytics dbt Project

**Simple, Scalable, Production-Ready**

This dbt project follows dbt Labs best practices and Kimball star schema methodology to transform rental property data into analytics-ready models. It runs on both Snowflake and BigQuery using warehouse-agnostic macros.

---

## Project Structure

```
models/
├── staging/          # Source-conformed data cleaning and standardization
│   ├── stg_listings.sql
│   ├── stg_calendar.sql
│   ├── stg_generated_reviews.sql
│   └── stg_amenities_changelog.sql
├── intermediate/    # Purpose-built transformation steps
│   ├── int_listings__current_amenities.sql
│   ├── int_calendar__daily_metrics.sql
│   └── int_listings__review_metrics.sql
└── marts/
    └── analytics/   # Business-conformed final tables (star schema)
        ├── dim_listings.sql       # Dimension: Listing attributes
        ├── dim_dates.sql          # Dimension: Date attributes
        ├── dim_hosts.sql          # Dimension: Host attributes
        ├── dim_neighborhoods.sql  # Dimension: Neighborhood attributes
        └── fct_listings_daily.sql # Fact: Daily listing metrics

analyses/            # Business question queries
├── business_question_1_amenity_revenue.sql
├── business_question_2_neighborhood_pricing.sql
├── business_question_3a_max_duration_stay.sql
└── business_question_3b_max_duration_with_amenities.sql

macros/              # Reusable SQL functions
├── cents_to_dollars.sql              # Business logic macros
├── parse_json_amenity.sql
├── parse_date.sql
├── parse_float.sql
└── warehouse_*.sql                   # Warehouse-agnostic macros
    ├── warehouse_cast.sql
    ├── warehouse_concat.sql
    ├── warehouse_date_trunc.sql
    ├── warehouse_date_diff.sql
    ├── warehouse_array_contains.sql
    └── warehouse_string_contains.sql
```

---

## Data Model (Kimball Star Schema)

### Staging Layer
- **stg_listings**: Cleans listing data, standardizes pricing
- **stg_calendar**: Converts availability flags, cleans pricing
- **stg_generated_reviews**: Standardizes review scores and dates
- **stg_amenities_changelog**: Cleans amenities JSON changelog

### Intermediate Layer
- **int_listings__current_amenities**: Determines current amenities for each listing-date using changelog
- **int_calendar__daily_metrics**: Calculates daily revenue and occupancy metrics
- **int_listings__review_metrics**: Aggregates review metrics per listing

### Marts Layer (Star Schema)

#### Dimensions (Descriptive Attributes)
- **dim_listings**: Listing attributes (property_type, room_type, beds, review metrics, etc.)
- **dim_dates**: Date attributes (year, quarter, month, day_of_week, is_weekend, etc.)
- **dim_hosts**: Host attributes (host_name, location, signup_date)
- **dim_neighborhoods**: Neighborhood reference

#### Facts (Measurements)
- **fct_listings_daily**: Daily listing metrics (grain: listing × date)
  - **Measures**: revenue_usd, price_usd, has_reservation, is_available
  - **Time-varying attributes**: has_air_conditioning, has_wifi, has_kitchen, etc. (7 amenity flags)
  - **Foreign keys**: listing_key, date_key, host_key, neighborhood_key

**Grain**: One row per listing per date

---

## Business Questions Answered

### 1. Amenity Revenue
Query: `analyses/business_question_1_amenity_revenue.sql`
- Total revenue and percentage by month segmented by air conditioning presence

### 2. Neighborhood Pricing
Query: `analyses/business_question_2_neighborhood_pricing.sql`
- Average price increase per neighborhood from July 12, 2021 to July 11, 2022

### 3. Long Stay / Picky Renter
- **3A**: Maximum duration stay per listing (`business_question_3a_max_duration_stay.sql`)
- **3B**: Maximum duration stay for listings with lockbox AND first aid kit (`business_question_3b_max_duration_with_amenities.sql`)

## Design Decisions

### 1. Kimball Star Schema ⭐
**Why**: Industry-standard dimensional modeling for analytics
- **Dimensions**: Descriptive attributes (listings, dates, hosts, neighborhoods)
- **Facts**: Measurements and metrics (revenue, occupancy, amenity flags)
- **Surrogate Keys**: Hashed keys for stability and flexibility
- **Benefits**: Simple queries, fast joins, easy to understand
- **Structure**: Fact table (`fct_listings_daily`) with foreign keys to 4 dimension tables

### 2. Warehouse-Agnostic 🌐
**Why**: Run on Snowflake OR BigQuery without code changes
- **Custom Macros**: `warehouse_cast`, `warehouse_concat`, `warehouse_date_trunc`, `warehouse_date_diff`, `warehouse_extract`, etc.
- **Parameterized**: Target-specific SQL generated at compile time
- **Normalized**: `day_of_week` normalized to 0-6 (0=Sunday, 6=Saturday) for both warehouses
- **Benefits**: Flexibility, no vendor lock-in, easier migration

### 3. Materializations 💾
- **Staging**: `view` - Always fresh, minimal storage
- **Intermediate**: `ephemeral` - Not materialized, compiled into downstream models
- **Marts**: `table` - Fast query performance, materialized for BI tools

### 4. Amenity Handling (Hybrid Approach) 🏠
**Why**: Balance between simplicity and flexibility
- **Boolean flags** for 7 key amenities in `fct_listings_daily` (fast queries)
- **Time-varying**: Amenities can change over time (tracked via changelog)
- **Point-in-time lookup**: Uses changelog to determine amenities per listing-date
- **Benefits**: Fast filtering, complete history, scalable

### 5. Naming Conventions 📝
- Staging: `stg_<source>__<entity>`
- Intermediate: `int_<entity>__<transformation>`
- Dimensions: `dim_<entity>`
- Facts: `fct_<entity>_<grain>`
- Macros: `warehouse_<function>` for agnostic functions

### 6. Key Implementation Decisions
- **Revenue**: Price if reserved, $0 if not
- **Occupancy**: `has_reservation` boolean flag (can calculate occupancy rate as AVG)
- **Amenities**: Tracked per date using changelog with point-in-time lookup (time-varying)
- **Reviews**: Aggregated per listing in `dim_listings` (not per date)
- **Primary Keys**: Surrogate keys for dimensions, composite for facts

---

## Quick Start

### 1. Install Dependencies
```bash
cd /path/to/project
dbt deps  # Install dbt_utils package
```

### 2. Configure Profile
Copy `profiles.yml.template` to `~/.dbt/profiles.yml` and configure:

**Snowflake**:
- Set account, user, password, role, warehouse, database, schema
- Separate `dev` and `prod` targets available

**BigQuery**:
- Set project_id, dataset, keyfile_path (service account JSON)
- Create service account with BigQuery Data Editor, Job User, and User roles
- Download keyfile and set path in profile

Update `dbt_project.yml` variable `warehouse_type` to match your target.

### 3. Load Data
Load CSVs to your warehouse `raw` schema (or use `dbt seed`):
- `LISTINGS.csv` → `raw.listings`
- `CALENDAR.csv` → `raw.calendar`
- `GENERATED_REVIEWS.csv` → `raw.generated_reviews`
- `AMENITIES_CHANGELOG.csv` → `raw.amenities_changelog`

### 4. Run dbt
```bash
dbt run      # Build all models
dbt test     # Run data quality tests
dbt docs generate && dbt docs serve  # View documentation
```

---

## Business Questions Answered

All queries in `analyses/` folder, ready to run:

1. **Amenity Revenue** (`business_question_1_amenity_revenue.sql`)
   - Monthly revenue by air conditioning presence

2. **Neighborhood Pricing** (`business_question_2_neighborhood_pricing.sql`)
   - Year-over-year price change by neighborhood

3. **Maximum Stay Duration** (`business_question_3a_max_duration_stay.sql`)
   - Longest possible stay per listing

4. **Maximum Stay with Amenities** (`business_question_3b_max_duration_with_amenities.sql`)
   - Longest stay for listings with lockbox AND first aid kit

---

## Warehouse-Agnostic Macros

All warehouse-specific functions use custom macros for compatibility:

| Macro | Purpose | Snowflake | BigQuery |
|-------|---------|-----------|----------|
| `warehouse_cast` | Type casting | `column::type` | `cast(column as type)` |
| `warehouse_concat` | String concatenation | `col1 \|\| col2` | `concat(col1, col2)` |
| `warehouse_date_trunc` | Date truncation | `date_trunc('month', date)` | `date_trunc(date, MONTH)` |
| `warehouse_date_diff` | Date difference | `datediff('day', start, end)` | `date_diff(end, start, DAY)` |
| `warehouse_extract` | Date extraction | `extract(dow from date)` | `extract(DAYOFWEEK from date) - 1` |
| `warehouse_string_contains` | String search | `LIKE '%value%'` | `LIKE '%value%'` |
| `warehouse_array_contains` | Array check | String matching | String matching |

**Note**: `day_of_week` is normalized to 0-6 (0=Sunday, 6=Saturday) for consistency.

---

## Key Features

✅ **Simple**: Clean 3-layer structure (staging → intermediate → marts)  
✅ **Scalable**: Star schema, modular design, reusable macros  
✅ **Production-Ready**: Tests, data contracts, documentation, parameterized, multi-warehouse  
✅ **Best Practices**: Follows dbt Labs standards and Kimball methodology  
✅ **Time-Aware**: Tracks amenity changes over time via changelog  
✅ **Data Contracts**: Schema enforcement enabled for all marts models  

---

## Project Variables

Configured in `dbt_project.yml`:
- `warehouse_type`: "snowflake" or "bigquery"
- `analysis_start_date`: Start date for date spine
- `analysis_end_date`: End date for date spine
- Schema names: `raw_schema`, `staging_schema`, `marts_schema`, etc.

---

## Contact

**Email**: saadq97.tech@gmail.com

