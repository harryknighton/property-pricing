"""Make price predictions for properties in the UK."""
import datetime
import logging

import geopandas as gpd
import statsmodels.api as sm
from sklearn.model_selection import train_test_split

from fynesse import assess, access
from fynesse.config import config

_TRAINING_FEATURES = ['price', 'property_type', 'latitude', 'longitude', 'distance_to_shop']
_CATEGORICAL_FEATURES = ['property_type']
_DATE_FORMAT_STR = '%Y-%m-%d'


def predict_price(latitude, longitude, date, property_type):
    """Predict a price for a house in the UK"""
    data = training_data(latitude, longitude, date)
    train_data, validation_data = train_test_split(data, test_size=config['test_size'])
    trained_model = train_model(train_data)
    mae = validate_model(trained_model, validation_data)
    if mae > 10000:
        logging.warning(f"MSE on validation data is {mae}")
    return make_prediction(trained_model, latitude, longitude, property_type)


def training_data(latitude, longitude, date):
    """Extend the data with OSM features for supervised learning."""
    north = latitude + config['training_bbox']
    south = latitude - config['training_bbox']
    east = longitude + config['training_bbox']
    west = longitude - config['training_bbox']
    six_months = datetime.timedelta(weeks=24)
    start_date = (date - six_months).strftime(_DATE_FORMAT_STR)
    end_date = (date + six_months).strftime(_DATE_FORMAT_STR)
    df = access.data((north, south, east, west), start_date, end_date)
    assess.validate_prices_data(df)
    df = access.attach_shop_distances(df)
    df = df[_TRAINING_FEATURES]
    df = assess.encode_categorical_features(df, _CATEGORICAL_FEATURES)
    return df


def train_model(df):
    y = df['price']
    x = df.drop('price', axis=1)
    model = sm.GLS(y, x)
    trained_model = model.fit_regularized(L1_wt=0.2, alpha=2)
    return trained_model


def validate_model(trained_model, df):
    y = df['price']
    x = df.drop('price', axis=1)
    predictions = trained_model.predict(x)
    return (y - predictions).abs().mean()


def make_prediction(trained_model, latitude, longitude, property_type):
    data = {
        'property_type': [property_type],
        'latitude': [latitude],
        'longitude': [longitude],
    }
    df = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy([longitude], [latitude], crs="EPSG:4326"))
    df = assess.encode_categorical_features(df, _CATEGORICAL_FEATURES)
    df_with_shops = access.attach_shop_distances(df)
    return trained_model.predict(df_with_shops.drop('geometry', axis=1))
