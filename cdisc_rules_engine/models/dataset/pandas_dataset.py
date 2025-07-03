from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
import pandas as pd
from typing import Union, List, Optional, Tuple


class PandasDataset(DatasetInterface):
    def __init__(self, data: pd.DataFrame = pd.DataFrame(), columns=None):
        self._data = data
        self.length = len(data)
        if columns and self._data.empty:
            self._data = pd.DataFrame(columns=columns)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def values(self):
        result = []
        for _, row_data in self.iterrows():
            result.append([row_data.get(col) for col in self.columns])
        return result

    @property
    def columns(self) -> List:
        return self._data.columns.to_list()

    @columns.setter
    def columns(self, columns):
        self._data.columns = columns

    @property
    def index(self):
        return self._data.index

    @property
    def groups(self):
        return self._data.groups

    @property
    def empty(self):
        return self._data.empty

    @property
    def loc(self):
        return self._data.loc

    @property
    def at(self):
        return self._data.at

    @property
    def iloc(self):
        return self._data.iloc

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        if "database_config" in kwargs.keys():
            kwargs.pop("database_config")
        dataframe = pd.DataFrame.from_dict(data, **kwargs)
        return cls(dataframe)

    @classmethod
    def from_records(cls, data: List[dict], **kwargs):
        if "database_config" in kwargs.keys():
            kwargs.pop("database_config")
        dataframe = pd.DataFrame.from_records(data, **kwargs)
        return cls(dataframe)

    def __getitem__(
        self, item: Union[str, List[str]]
    ) -> Union[pd.Series, pd.DataFrame]:
        return self._data[item]

    def __setitem__(self, key, value: pd.Series):
        self._data[key] = value

    def __len__(self):
        return len(self._data)

    def __contains__(self, item: str) -> bool:
        return item in self._data

    def get(self, target: Union[str, List[str]], default=None):
        return self._data.get(target, default)

    def groupby(self, by: List[str], **kwargs):
        return self.__class__(self._data.groupby(by, **kwargs))

    def get_grouped_size(self, by, **kwargs):
        grouped_data = self._data.groupby(by, **kwargs)
        return grouped_data.size()

    def is_column_sorted_within(self, group, column):
        return (
            False
            not in self.groupby(group)[column]
            .apply(list)
            .map(lambda x: sorted(x) == x)
            .values
        )

    def concat(self, other: Union[DatasetInterface, List[DatasetInterface]], **kwargs):
        if isinstance(other, list):
            new_data = self._data.copy()
            for dataset in other:
                new_data = pd.concat([new_data, dataset.data], **kwargs)
        else:
            new_data = pd.concat([self._data, other.data], **kwargs)
        return self.__class__(new_data)

    def merge(self, other: DatasetInterface, **kwargs):
        new_data = self._data.merge(other, **kwargs)
        return self.__class__(new_data)

    def apply(self, func, **kwargs):
        return self._data.apply(func, **self._remove_invalid_kwargs(["meta"], kwargs))

    def iterrows(self):
        return self._data.iterrows()

    @classmethod
    def is_series(cls, data) -> bool:
        return isinstance(data, pd.Series)

    @classmethod
    def get_series_values(cls, series) -> list:
        if not cls.is_series(series):
            return []
        return series.values

    def rename(self, index=None, columns=None, inplace=True):
        self._data.rename(index=index, columns=columns, inplace=inplace)
        return self

    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """
        Drop specified labels from rows or columns.
        """
        self._data = self._data.drop(
            labels=labels, axis=axis, columns=columns, errors=errors
        )
        return self

    def melt(
        self,
        id_vars=None,
        value_vars=None,
        var_name=None,
        value_name="value",
        col_level=None,
    ):
        """
        Unpivots a DataFrame from wide format to long format,
        optionally leaving identifier variables set.
        """
        new_data = self._data.melt(
            id_vars=id_vars,
            var_name=var_name,
            value_vars=value_vars,
            value_name=value_name,
            col_level=col_level,
        )
        return self.__class__(new_data)

    def set_index(self, keys, **kwargs):
        new_data = self._data.set_index(keys, **kwargs)
        return self.__class__(new_data)

    def filter(self, **kwargs):
        new_data = self._data.filter(**kwargs)
        return self.__class__(new_data)

    def convert_to_series(self, result):
        if self.is_series(result):
            return result
        return pd.Series(result)

    def get_series_from_value(self, result):
        if hasattr(result, "__iter__"):
            return pd.Series([result] * len(self._data), index=self._data.index)
        return pd.Series(result, index=self._data.index)

    def _remove_invalid_kwargs(self, invalid_args, kwargs) -> dict:
        for arg in invalid_args:
            if arg in kwargs:
                del kwargs[arg]
        return kwargs

    def len(self) -> int:
        return self._data.shape[0]

    @property
    def size(self) -> int:
        return self._data.memory_usage().sum()

    def copy(self):
        new_data = self._data.copy()
        return self.__class__(new_data)

    def equals(self, other_dataset: DatasetInterface):
        return self._data.equals(other_dataset.data)

    def get_error_rows(self, results) -> "pd.Dataframe":
        data_with_results = self._data.copy()
        data_with_results["results"] = results
        return data_with_results[data_with_results["results"].isin([True])]

    def where(self, cond, other, **kwargs):
        """
        Wrapper for dataframe where function
        """
        new_data = self._data.where(cond, other, **kwargs)
        return self.__class__(new_data)

    @classmethod
    def cartesian_product(cls, left, right):
        """
        Return the cartesian product of two dataframes
        """
        return cls(left.merge(right, how="cross"))

    def sort_values(self, by: Union[str, list[str]], **kwargs) -> "pd.Dataframe":
        """
        Sort the underlying dataframe and return a raw dataframe.
        """
        return self._data.sort_values(by, **kwargs)

    def dropna(self, inplace=False, **kwargs):
        result = self._data.dropna(**kwargs)
        if inplace:
            self._data = result
            return None
        else:
            return self.__class__(result)

    def drop_duplicates(self, subset=None, keep="first", inplace=False, **kwargs):
        """
        Drop duplicate rows from the dataset.
        """
        new_data = self._data.drop_duplicates(
            subset=subset, keep=keep, inplace=inplace, **kwargs
        )
        return self.__class__(new_data)

    def replace(self, to_replace, value, **kwargs):
        self._data = self._data.replace(to_replace, value, **kwargs)
        return self

    def astype(self, dtype, **kwargs):
        self._data = self._data.astype(dtype, **kwargs)
        return self

    def min(self, *args, **kwargs):
        return self.__class__(self._data.min(*args, **kwargs))

    def reset_index(self, drop=False, **kwargs):
        """
        Reset the index of the dataset.
        """
        self._data = self._data.reset_index(drop=drop, **kwargs)
        return self

    def fillna(
        self,
        value=None,
        method=None,
        axis=None,
        inplace=False,
        limit=None,
        downcast=None,
    ):
        """
        Fill NA/NaN values using the specified method.
        """
        result = self._data.fillna(
            value=value,
            method=method,
            axis=axis,
            inplace=inplace,
            limit=limit,
            downcast=downcast,
        )
        if inplace:
            return None
        else:
            return self.__class__(result)

    def to_dict(self, **kwargs) -> dict:
        return self._data.to_dict(**kwargs)

    def unique(self, column: Optional[str] = None):
        """Return unique values of Series or DataFrame column."""
        if column is not None:
            return self._data[column].unique()
        # If called on series-like data
        if len(self._data.columns) == 1:
            return self._data.iloc[:, 0].unique()
        raise ValueError("Must specify column for DataFrame with multiple columns")

    def squeeze(self, axis=None):
        """Squeeze 1-dimensional axis objects into scalars."""
        result = self._data.squeeze(axis=axis)
        if isinstance(result, pd.Series) or isinstance(result, pd.DataFrame):
            return self.__class__(result)
        return result  # scalar

    def duplicated(self, subset=None, keep="first"):
        """Return boolean Series denoting duplicate rows."""
        return self._data.duplicated(subset=subset, keep=keep)

    def isna(self):
        """Detect missing values."""
        result = self._data.isna()
        return self.__class__(result)

    def notna(self):
        """Detect non-missing values."""
        result = self._data.notna()
        return self.__class__(result)

    def head(self, n=5):
        """Return the first n rows."""
        return self.__class__(self._data.head(n))

    def tail(self, n=5):
        """Return the last n rows."""
        return self.__class__(self._data.tail(n))

    def nunique(self, axis=0, dropna=True):
        """Count distinct observations."""
        return self._data.nunique(axis=axis, dropna=dropna)

    def agg(self, func, axis=0, *args, **kwargs):
        """Aggregate using one or more operations."""
        result = self._data.agg(func, axis=axis, *args, **kwargs)
        if isinstance(result, (pd.DataFrame, pd.Series)):
            return self.__class__(result)
        return result

    def select_dtypes(self, include=None, exclude=None):
        """Return a subset of the DataFrame's columns based on the column dtypes."""
        result = self._data.select_dtypes(include=include, exclude=exclude)
        return self.__class__(result)

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        """Return cumulative sum."""
        result = self._data.cumsum(axis=axis, skipna=skipna, *args, **kwargs)
        return self.__class__(result)

    def to_frame(self, name=None):
        """Convert Series to DataFrame."""
        if isinstance(self._data, pd.Series):
            result = self._data.to_frame(name=name)
            return self.__class__(result)
        return self  # Already a DataFrame

    def describe(self, percentiles=None, include=None, exclude=None):
        """Generate descriptive statistics."""
        result = self._data.describe(
            percentiles=percentiles, include=include, exclude=exclude
        )
        return self.__class__(result)

    def value_counts(
        self, normalise=False, sort=True, ascending=False, bins=None, dropna=True
    ):
        """Return a Series containing counts of unique values."""
        if len(self._data.columns) == 1:
            result = self._data.iloc[:, 0].value_counts(
                normalize=normalise,
                sort=sort,
                ascending=ascending,
                bins=bins,
                dropna=dropna,
            )
            return pd.Series(result)
        raise ValueError("value_counts() only works on single columns")

    def shift(self, periods=1, freq=None, axis=0, fill_value=None):
        """Shift index by desired number of periods."""
        result = self._data.shift(
            periods=periods, freq=freq, axis=axis, fill_value=fill_value
        )
        return self.__class__(result)

    @property
    def shape(self) -> Tuple[int, int]:
        """Return a tuple representing the dimensionality of the dataset."""
        return self._data.shape

    @property
    def dtypes(self):
        """Return the dtypes in the dataset."""
        return self._data.dtypes

    @property
    def values(self):
        """Return a Numpy representation of the data."""
        return self._data.values

    @property
    def str(self):
        """Vectorised string functions for Series and Index."""

        # For DataFrame, we need to handle column selection
        class StringAccessor:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key].str

            def __getattr__(self, item):
                # If accessing on entire DataFrame, raise error
                if isinstance(self._data, pd.DataFrame):
                    raise AttributeError(
                        "Can only use .str accessor with string values!"
                    )
                return getattr(self._data.str, item)

        return StringAccessor(self._data)

    @property
    def dt(self):
        """Accessor object for datetime-like properties."""

        # Similar to str accessor
        class DatetimeAccessor:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key].dt

            def __getattr__(self, item):
                if isinstance(self._data, pd.DataFrame):
                    raise AttributeError(
                        "Can only use .dt accessor with datetimelike values!"
                    )
                return getattr(self._data.dt, item)

        return DatetimeAccessor(self._data)

    def isin(self, values):
        """Check whether each element is contained in values."""
        result = self._data.isin(values)
        return self.__class__(result)

    def iloc(self, row_indexer, col_indexer=None):
        """Purely integer-location based indexing for selection by position."""
        if col_indexer is None:
            result = self._data.iloc[row_indexer]
        else:
            result = self._data.iloc[row_indexer, col_indexer]

        if isinstance(result, (pd.DataFrame, pd.Series)):
            return self.__class__(result)
        return result

    def map(self, mapper, na_action=None):
        """Map values using an input mapping or function."""
        if len(self._data.columns) == 1:
            # Map on single column
            result = self._data.iloc[:, 0].map(mapper, na_action=na_action)
            return pd.Series(result)
        else:
            # For DataFrame, apply map to each column
            result = self._data.apply(lambda x: x.map(mapper, na_action=na_action))
            return self.__class__(result)

    def to_parquet(self, path=None, **kwargs):
        """Write a DataFrame to the parquet format."""
        return self._data.to_parquet(path, **kwargs)

    def assign(self, **kwargs):
        """Assign new columns to a DataFrame."""
        result = self._data.assign(**kwargs)
        return self.__class__(result)

    def eq(self, other, axis="columns", level=None):
        """Get Equal to of dataframe and other, element-wise."""
        result = self._data.eq(other, axis=axis, level=level)
        return self.__class__(result)

    def ne(self, other, axis="columns", level=None):
        """Get Not equal to of dataframe and other, element-wise."""
        result = self._data.ne(other, axis=axis, level=level)
        return self.__class__(result)

    def lt(self, other, axis="columns", level=None):
        """Get Less than of dataframe and other, element-wise."""
        result = self._data.lt(other, axis=axis, level=level)
        return self.__class__(result)

    def le(self, other, axis="columns", level=None):
        """Get Less than or equal to of dataframe and other, element-wise."""
        result = self._data.le(other, axis=axis, level=level)
        return self.__class__(result)

    def gt(self, other, axis="columns", level=None):
        """Get Greater than of dataframe and other, element-wise."""
        result = self._data.gt(other, axis=axis, level=level)
        return self.__class__(result)

    def ge(self, other, axis="columns", level=None):
        """Get Greater than or equal to of dataframe and other, element-wise."""
        result = self._data.ge(other, axis=axis, level=level)
        return self.__class__(result)

    def any(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether any element is True."""
        return self._data.any(
            axis=axis, bool_only=bool_only, skipna=skipna, level=level, **kwargs
        )

    def all(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether all elements are True."""
        return self._data.all(
            axis=axis, bool_only=bool_only, skipna=skipna, level=level, **kwargs
        )

    def sum(
        self,
        axis=None,
        skipna=True,
        level=None,
        numeric_only=None,
        min_count=0,
        **kwargs,
    ):
        """Return the sum of the values."""
        result = self._data.sum(
            axis=axis,
            skipna=skipna,
            level=level,
            numeric_only=numeric_only,
            min_count=min_count,
            **kwargs,
        )
        if isinstance(result, pd.Series):
            return result
        return result

    def mean(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the mean of the values."""
        result = self._data.mean(
            axis=axis, skipna=skipna, level=level, numeric_only=numeric_only, **kwargs
        )
        if isinstance(result, pd.Series):
            return result
        return result

    def std(
        self, axis=None, skipna=True, level=None, ddof=1, numeric_only=None, **kwargs
    ):
        """Return sample standard deviation."""
        result = self._data.std(
            axis=axis,
            skipna=skipna,
            level=level,
            ddof=ddof,
            numeric_only=numeric_only,
            **kwargs,
        )
        if isinstance(result, pd.Series):
            return result
        return result

    def max(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the maximum of the values."""
        result = self._data.max(
            axis=axis, skipna=skipna, level=level, numeric_only=numeric_only, **kwargs
        )
        if isinstance(result, pd.Series):
            return result
        return result

    def round(self, decimals=0, *args, **kwargs):
        """Round to a variable number of decimal places."""
        result = self._data.round(decimals, *args, **kwargs)
        return self.__class__(result)
