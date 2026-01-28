# Mycorr Python API Design Principles

## Purpose

The mycorr API should enable developers and data scientists to access data in mycorr using a simple python interface. While not allowing to mutate data, this interface enables convenient streaming of tabular data directly from mycorr's backend.

## Authentication

The user authenticates with a bearer token they can generate in the mycorr UI.
