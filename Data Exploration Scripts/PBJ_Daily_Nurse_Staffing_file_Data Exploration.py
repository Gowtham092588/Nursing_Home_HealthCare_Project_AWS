import pandas as pd
import matplotlib.pyplot as plt
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import StringType

spark = SparkContext()

df = pd.read_csv(
    r'c:\Users\yamun\Downloads\HealthCare\PBJ_Daily_Nurse_Staffing_Q2_2024.csv', encoding='cp1252')

# No of Rows and Columns in CSV

print(df.shape)

print(df["PROVNUM"].dtype)

print(df["PROVNUM"].apply(type).value_counts())

print(df.schema)

df.select(
    "county_code",
    F.col("county_code").isNull().alias("is_null")
).show()

# [1325324 rows x 33 columns]

# Selecting first 5 rows to check on Formatting.

print("\n..... First 5 Rows ........")
print(df.head())

#     PROVNUM      PROVNAME                       CITY STATE COUNTY_NAME  COUNTY_FIPS  CY_Qtr  WorkDate  MDScensus  Hrs_RNDON  Hrs_RNDON_emp  Hrs_RNDON_ctr  Hrs_RNadmin  Hrs_RNadmin_emp  Hrs_RNadmin_ctr  ...  Hrs_LPNadmin  Hrs_LPNadmin_emp  Hrs_LPNadmin_ctr  Hrs_LPN  Hrs_LPN_emp  Hrs_LPN_ctr  Hrs_CNA  Hrs_CNA_emp  Hrs_CNA_ctr  Hrs_NAtrn  Hrs_NAtrn_emp  Hrs_NAtrn_ctr  Hrs_MedAide  Hrs_MedAide_emp  Hrs_MedAide_ctr
# 0   15009  BURNS NURSING HOME, INC.  RUSSELLVILLE    AL    Franklin           59  2024Q2  20240401         51      10.77          10.77            0.0        10.40            10.40              0.0  ...           0.0               0.0               0.0    25.50        25.50          0.0   160.08       160.08          0.0        0.0            0.0            0.0          0.0              0.0              0.0
# 1   15009  BURNS NURSING HOME, INC.  RUSSELLVILLE    AL    Franklin           59  2024Q2  20240402         52       8.43           8.43            0.0        18.25            18.25              0.0  ...           0.0               0.0               0.0    15.22        15.22          0.0   135.95       135.95          0.0        0.0            0.0            0.0          0.0              0.0              0.0
# 2   15009  BURNS NURSING HOME, INC.  RUSSELLVILLE    AL    Franklin           59  2024Q2  20240403         53      11.13          11.13            0.0        12.08            12.08              0.0  ...           0.0               0.0               0.0     5.46         5.46          0.0   150.31       150.31          0.0        0.0            0.0            0.0          0.0              0.0              0.0
# 3   15009  BURNS NURSING HOME, INC.  RUSSELLVILLE    AL    Franklin           59  2024Q2  20240404         52      12.27          12.27            0.0        17.53            17.53              0.0  ...           0.0               0.0               0.0    20.18        20.18          0.0   133.01       133.01          0.0        0.0            0.0            0.0          0.0              0.0              0.0
# 4   15009  BURNS NURSING HOME, INC.  RUSSELLVILLE    AL    Franklin           59  2024Q2  20240405         52       4.95           4.95            0.0        17.42            17.42              0.0  ...           0.0               0.0               0.0    27.85        27.85          0.0   137.92       137.92          0.0        0.0            0.0            0.0          0.0              0.0              0.0

print(df["PROVNUM"].head(20))

# To check metadata of the table

print("--- Data Schema Profile ---")
df.info()

# RangeIndex: 1325324 entries, 0 to 1325323
# Data columns (total 33 columns):
#  #   Column            Non-Null Count    Dtype
# ---  ------            --------------    -----
#  0   PROVNUM           1325324 non-null  object
#  1   PROVNAME          1325324 non-null  str
#  2   CITY              1325324 non-null  str
#  3   STATE             1325324 non-null  str
#  4   COUNTY_NAME       1325324 non-null  str
#  5   COUNTY_FIPS       1325324 non-null  int64
#  6   CY_Qtr            1325324 non-null  str
#  7   WorkDate          1325324 non-null  int64
#  8   MDScensus         1325324 non-null  int64
#  9   Hrs_RNDON         1325324 non-null  float64
#  10  Hrs_RNDON_emp     1325324 non-null  float64
#  11  Hrs_RNDON_ctr     1325324 non-null  float64
#  12  Hrs_RNadmin       1325324 non-null  float64
#  13  Hrs_RNadmin_emp   1325324 non-null  float64
#  14  Hrs_RNadmin_ctr   1325324 non-null  float64
#  15  Hrs_RN            1325324 non-null  float64
#  16  Hrs_RN_emp        1325324 non-null  float64
#  17  Hrs_RN_ctr        1325324 non-null  float64
#  18  Hrs_LPNadmin      1325324 non-null  float64
#  19  Hrs_LPNadmin_emp  1325324 non-null  float64
#  20  Hrs_LPNadmin_ctr  1325324 non-null  float64
#  21  Hrs_LPN           1325324 non-null  float64
#  22  Hrs_LPN_emp       1325324 non-null  float64
#  23  Hrs_LPN_ctr       1325324 non-null  float64
#  24  Hrs_CNA           1325324 non-null  float64
#  25  Hrs_CNA_emp       1325324 non-null  float64
#  26  Hrs_CNA_ctr       1325324 non-null  float64
#  27  Hrs_NAtrn         1325324 non-null  float64
#  28  Hrs_NAtrn_emp     1325324 non-null  float64
#  29  Hrs_NAtrn_ctr     1325324 non-null  float64
#  30  Hrs_MedAide       1325324 non-null  float64
#  31  Hrs_MedAide_emp   1325324 non-null  float64
#  32  Hrs_MedAide_ctr   1325324 non-null  float64
# dtypes: float64(24), int64(3), object(1), str(5)
# memory usage: 333.7+ MB

# Duplicate Rows

duplicated = df.duplicated().sum()

print(f'\nTotal no of duplicated rows:{duplicated}')

# Total no of duplicated rows:0

# Check for missing values

missing_values = df.isnull().sum()
print("\n--- Missing Values Per Column ---")
missing_data = missing_values[missing_values > 0].sort_values(ascending=False)

if missing_data.empty:
    print(" High-five! Your dataset has absolutely ZERO missing values.")
    print("No bar chart generated because there are no null gaps to display.")
else:
    print(
        f"Found missing records across {len(missing_data)} different columns.")

    plt.figure(figsize=(10, 6))
    missing_data.plot(kind='bar', color='#e74c3c', edgecolor='black')

    # 5. Customize the chart aesthetics
    plt.title('Missing Values Count Per Column',
              fontsize=14, fontweight='bold')
    plt.xlabel('Columns / Features', fontsize=12)
    plt.ylabel('Number of Missing Records (Nulls)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.show()


# --- Missing Values Per Column ---
# PROVNUM             0
# PROVNAME            0
# CITY                0
# STATE               0
# COUNTY_NAME         0
# COUNTY_FIPS         0
# CY_Qtr              0
# WorkDate            0
# MDScensus           0
# Hrs_RNDON           0
# Hrs_RNDON_emp       0
# Hrs_RNDON_ctr       0
# Hrs_RNadmin         0
# Hrs_RNadmin_emp     0
# Hrs_RNadmin_ctr     0
# Hrs_RN              0
# Hrs_RN_emp          0
# Hrs_RN_ctr          0
# Hrs_LPNadmin        0
# Hrs_LPNadmin_emp    0
# Hrs_LPNadmin_ctr    0
# Hrs_LPN             0
# Hrs_LPN_emp         0
# Hrs_LPN_ctr         0
# Hrs_CNA             0
# Hrs_CNA_emp         0
# Hrs_CNA_ctr         0
# Hrs_NAtrn           0
# Hrs_NAtrn_emp       0
# Hrs_NAtrn_ctr       0
# Hrs_MedAide         0
# Hrs_MedAide_emp     0
# Hrs_MedAide_ctr     0

# Unique

df_clean = df['PROVNAME'].unique()
print(df_clean)

# Statistical Distribution

print("\n--- Statistical Distribution ---")
print(df.describe())
