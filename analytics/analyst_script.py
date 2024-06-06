import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from prophet import Prophet

def load_data():
    """Load measurement data from CSV files."""
    measurement_info = pd.read_csv("/home/kuanysh/work/repos/DE-DS-BI/dataset/Measurement_info.csv")
    measurement_item_info = pd.read_csv("/home/kuanysh/work/repos/DE-DS-BI/dataset/Measurement_item_info.csv")
    measurement_station_info = pd.read_csv("/home/kuanysh/work/repos/DE-DS-BI/dataset/Measurement_station_info.csv")

    measurement_info.rename(columns={'Station code':'Station_code', 'Instrument status':'Instrument_status', 'Item code':'Item_code'}, inplace=True)
    measurement_item_info.rename(columns={'Item code':'Item_code'}, inplace=True)

    return measurement_info, measurement_item_info, measurement_station_info

def generate_evaluation_functions(measurement_item_info):
    """Create a dictionary of evaluation functions for each pollutant."""
    evaluators = {
        row['Item_code']: evaluation_generator(row['Good(Blue)'], row['Normal(Green)'], row['Bad(Yellow)'], row['Very bad(Red)'])
        for idx, row in measurement_item_info.iterrows()
    }
    return evaluators

def evaluation_generator(good, normal, bad, vbad):
    """Generate a pollution level evaluation function."""
    def measurement_evaluator(value):
        if pd.isnull(value) or value < 0:
            return np.nan
        elif value <= good:
            return 'Good'
        elif value <= normal:
            return 'Average'
        elif value <= bad:
            return 'Bad'
        else:
            return 'Worst'
    return measurement_evaluator

def preprocess_data(measurement_info, measurement_item_info):
    """Preprocess the measurement data."""
    measurement_info = measurement_info.loc[measurement_info['Instrument_status'] == 0]
    measurement_new = measurement_info.merge(measurement_item_info, on="Item_code", how="left")

    return measurement_new

def add_date_columns(measurement_new):
    """Add columns for date, month, and year."""
    measurement_new['Measurement date'] = pd.to_datetime(measurement_new['Measurement date'])
    measurement_new['YearMonth'] = measurement_new['Measurement date'].dt.to_period('M')
    return measurement_new

def plot_heatmap(data, save_path):
    """Visualize average pollution levels by stations and months."""
    plt.figure(figsize=(15, 10))
    sns.heatmap(data, cmap='viridis', linecolor='white', linewidths=0.1)
    plt.title('Average Pollution Levels by Station and Month')
    plt.xlabel('Month')
    plt.ylabel('Station Code')
    plt.xticks(rotation=90)
    plt.savefig(save_path)
    plt.close()

def plot_correlation(data, save_path):
    """Visualize the correlation between SO2 and NO2."""
    plt.figure(figsize=(6, 4))
    ax = sns.heatmap(data=data, annot=True, cmap=sns.color_palette("coolwarm", 7), vmin=-1, vmax=1)
    plt.title('Correlation Matrix between NO2 and SO2', fontsize=16, fontweight='bold')
    plt.xticks(rotation=0)
    plt.yticks(rotation=0)
    plt.savefig(save_path)
    plt.close()

def plot_bar_chart(data, save_path):
    """Plot a bar chart."""
    agg_data = data.groupby(['Weekend', 'Status']).size().unstack(fill_value=0)
    agg_data.plot(kind='bar', stacked=True)
    plt.ylabel('Observation Count')
    plt.xlabel('Day (0 - Weekdays, 1 - Weekends)')
    plt.title('Pollution Level by Days of the Week')
    plt.xticks([0, 1], ['Weekdays', 'Weekends'], rotation=0)
    plt.legend(title='Pollution Level')
    plt.yscale('log')
    plt.savefig(save_path)
    plt.close()

def main():
    measurement_info, measurement_item_info, _ = load_data()
    evaluators = generate_evaluation_functions(measurement_item_info)
    measurement_new = preprocess_data(measurement_info, measurement_item_info)
    measurement_new['Status'] = measurement_new.apply(lambda row: evaluators[row['Item_code']](row['Average value']), axis=1)
    measurement_new = add_date_columns(measurement_new)

    # Group data by stations and months, compute average pollution levels (Free analysis)
    station_monthly_trends = measurement_new.groupby(['Station_code', 'YearMonth'])['Average value'].mean().unstack().fillna(0)
    plot_heatmap(station_monthly_trends, 'station_monthly_trends.png')

    so2_no2_data = measurement_new[measurement_new['Item name'].isin(['SO2', 'NO2'])]

    # Pivot table for proper data representation
    pivot_data = so2_no2_data.pivot_table(values='Average value', index=['Measurement date', 'Station_code'], columns='Item name').reset_index()

    # Ensure there are no missing values in SO2 and NO2 columns
    pivot_data.dropna(subset=['SO2', 'NO2'], inplace=True)

    # Correlation analysis
    correlations = pivot_data[['SO2', 'NO2']].corr(method='spearman')
    correlation_coefficient = correlations.loc['SO2', 'NO2']
    print("Correlation Coefficient (SO2 vs NO2):", correlation_coefficient)
    plot_correlation(correlations, 'corr_no2_so2.png')

    measurement_not_good = measurement_new[measurement_new['Status']!="Good"]
    measurement_not_good['Weekend'] = ((pd.DatetimeIndex(measurement_not_good['Measurement date']).dayofweek) // 5 == 1).astype(int)
    measurement_not_good['Month'] = pd.DatetimeIndex(measurement_not_good['Measurement date']).month
    
    # Hypothesis testing
    print(measurement_not_good['Weekend'].value_counts())
    plot_bar_chart(measurement_not_good, 'weekend_vs_weekdays.png')

    # Predictive Modeling 
    measurement_station = measurement_info[(measurement_info['Station_code']==109)&(measurement_info['Item_code']==9)]
    model = Prophet()
    data = measurement_station[['Measurement date','Average value']]
    data['Measurement date'] = pd.to_datetime(data['Measurement date'])
    data.rename(columns={'Measurement date':'ds','Average value':'y'},inplace=True)

    model.fit(data)
    future = model.make_future_dataframe(periods=380)
    prediction = model.predict(future)
    print(prediction.head())
    prediction = prediction[['ds','yhat_lower','yhat_upper','yhat']]
    plt.figure(figsize=(10, 6))
    plt.plot(data['ds'], data['y'], label='measured data', color='blue')
    plt.plot(prediction['ds'], prediction['yhat'], label='prediction', color='red')
    plt.fill_between(prediction['ds'], prediction['yhat_lower'], prediction['yhat_upper'], color='pink', alpha=0.3)
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.title('Prediction')
    plt.legend()
    plt.grid(True)
    plt.savefig("forecast_custom.png")
    plt.show()

if __name__ == "__main__":
    main()
