"""
Model to use is linear regression (y=ax+b)
The input
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers


class Model:
    def __init__(self):
        self.train_features = None
        self.test_features = None
        self.train_labels = None
        self.test_labels = None

    def create_model(self):
        df = self.read_and_clean_dataset()
        self.split_dataset(df)
        self.configure_and_output_model()

    @staticmethod
    def read_and_clean_dataset():
        df = pd.read_csv("UK Gender Pay Gap Data - 2021 to 2022.csv")
        # drop irrelevant columns
        df = df.iloc[:, 6:]
        df = df.iloc[:, :-4]
        # specify other cols to drop
        df = df.drop(columns=['CompanyLinkToGPGInfo', 'ResponsiblePerson'])
        # one hot encoding for employerSize. the target col is the diff meanHourlyPercent
        employer_size_categ = list(df['EmployerSize'].unique())
        df["EmployerSize"] = df["EmployerSize"].replace({val: idx for idx, val in enumerate(employer_size_categ)})
        df = df.dropna()
        return df

    def split_dataset(self, df):
        train_dataset = df.sample(frac=0.8, random_state=0)
        test_dataset = df.drop(train_dataset.index)
        # remove the label from the feature
        self.train_features = train_dataset.copy()
        self.test_features = test_dataset.copy()

        self.train_labels = self.train_features.pop('DiffMeanHourlyPercent')
        self.test_labels = self.test_features.pop('DiffMeanHourlyPercent')

    def configure_and_output_model(self):
        # normalization
        normalizer = tf.keras.layers.Normalization(axis=-1)
        normalizer.adapt(np.array(self.train_features))

        # create the linear model
        linear_model = tf.keras.Sequential([
            normalizer,
            layers.Dense(units=1)
        ])

        # configure the model
        linear_model.compile(
            optimizer=tf.optimizers.Adam(learning_rate=0.1),
            loss='mean_absolute_error')

        #  train the model
        linear_model.fit(self.train_features, self.train_labels, epochs=200, verbose=0, validation_split=0.2)
        # evaluate the model using the test dataset
        mean_absolute_error = linear_model.evaluate(
            self.test_features, self.test_labels, verbose=0)
        print(f"After building the model, the error is {mean_absolute_error}")
        linear_model.save('linear_model')
        print("Model is created")

if __name__ == '__main__':
    model = Model()
    model.create_model()

