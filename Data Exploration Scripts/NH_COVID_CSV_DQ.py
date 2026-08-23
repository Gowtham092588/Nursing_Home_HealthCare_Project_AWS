import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(
    r'C:\Users\yamun\Downloads\HealthCare\NH_CovidVaxAverages_20241027.csv', encoding='cp1252')

# No of Rows and Columns in CSV

print(df.shape)


# Selecting first 5 rows to check on Formatting.

print("\n..... First 5 Rows ........")
print(df.head())

# To check metadata of the table

print("--- Data Schema Profile ---")
df.info()

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

# Unique

# df_clean = df['PROVNAME'].unique()
# print(df_clean)

# Statistical Distribution

print("\n--- Statistical Distribution ---")
print(df.describe())
