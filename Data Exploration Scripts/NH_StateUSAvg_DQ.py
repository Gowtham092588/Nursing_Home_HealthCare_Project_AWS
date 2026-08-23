import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(
    r'c:\Users\yamun\Downloads\HealthCare\NH_StateUSAverages_Oct2024.csv', encoding='cp1252')

Unique

df_clean = df['CMS Certification Number (CCN)'].nunique()
print(df_clean)

Returns each CCN alongside its total row count

ccn_distribution = df["CMS Certification Number (CCN)"].value_counts()
print(ccn_distribution)

No of Rows and Columns in CSV

print(df.shape)

print("\n..... First 5 Rows ........")
print(df.head())


# To check metadata of the table

print("--- Data Schema Profile ---")
df.info()

for col in df.columns:
    print(col)

# Duplicate Rows

duplicated = df.duplicated().sum()

print(f'\nTotal no of duplicated rows:{duplicated}')

# Check for missing values

null_summary = {}

for c in df.columns:
    cnt = df[c].isnull().sum()
    if cnt > 0:
        null_summary[c] = cnt

print(null_summary)

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

Statistical Distribution

print("\n--- Statistical Distribution ---")
print(df.describe())
