"""Provide tools to visualise and validate data."""

import datetime

import numpy as np
from pandera import Column, Check
import seaborn as sns
import pandera as pa
from sklearn import decomposition

_prices_coordinates_data_schema = schema = pa.DataFrameSchema({
    'price': Column(int, checks=Check.in_range(0, 10e8)),
    'date_of_transfer': Column(pa.Date, checks=Check.in_range(datetime.date(2018, 1, 1), datetime.date(2023, 1, 1))),
    'postcode': Column(str, checks=Check.str_length(0, 8)),
    'property_type': Column(str, checks=Check.isin(['F', 'S', 'D', 'T', 'O'])),
    'new_build_flag': Column(str, checks=Check.isin(['Y', 'N'])),
    'tenure_type': Column(str, checks=Check.isin(['F', 'L'])),
    'locality': Column(str, nullable=True),
    'town_city': Column(str),
    'district': Column(str, nullable=True),
    'county': Column(str, nullable=True),
    'country': Column(str),
    'latitude': Column(float, checks=Check.in_range(49, 61)),
    'longitude': Column(float, checks=Check.in_range(-8, 2)),
    'db_id': Column(int, unique=True),
})


def validate_prices_data(df):
    """Validate data using the schema"""
    return _prices_coordinates_data_schema.validate(df)


def normalise_numerical_features(df, features):
    """Normalise the numerical features so that they fall in roughly the same range."""
    normalised_df = df.copy()
    for feature in features:
        normalised_df[feature] -= normalised_df[feature].mean()
        normalised_df[feature] /= normalised_df[feature].std()
    return normalised_df


def encode_categorical_features(df, features):
    """Ordinal encode the categorical features in the data"""
    encoded_df = df.copy()
    for feature in features:
        encoded_df[feature] = encoded_df[feature].astype('category').cat.codes
    return encoded_df


def visualise_correlation(df):
    """Produce a heatmap of the correlation matrix between features."""
    sns.heatmap(df.corr(), annot=True)


def perform_pca(df, features, num_components=None):
    """Perform a PCA and calculate the reponsibility of each original feature for the variance"""
    pca = decomposition.PCA(num_components)
    pca.fit(df[features].values)
    contributions = np.sum(pca.components_ * pca.explained_variance_ratio_, axis=1)
    return {attr: contrib for attr, contrib in zip(list(df[features].columns), contributions)}



