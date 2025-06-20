from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np
import re
import dask
from typing import List, Union, Optional, Tuple

DEFAULT_NUM_PARTITIONS = 4
dask.config.set({"dataframe.convert-string": False})


class DaskDataset(PandasDataset):
    def __init__(
        self,
        data=dd.from_pandas(pd.DataFrame(), npartitions=DEFAULT_NUM_PARTITIONS),
        columns=None,
        length=None,
    ):
        self._data = data
        self.length = length
        if columns and self._data.empty:
            self._data = dd.from_pandas(
                pd.DataFrame(columns=columns), npartitions=DEFAULT_NUM_PARTITIONS
            )

    @property
    def data(self):
        return self._data

    @property
    def loc(self):
        return self._data.loc

    @property
    def size(self):
        memory_usage = self.data.get_partition(0).compute().memory_usage()
        return memory_usage.sum()

    @data.setter
    def data(self, data):
        self._data = data

    def __getitem__(self, item):
        try:
            return self._data[item].compute().reset_index(drop=True)
        except ValueError as e:
            # Handle boolean indexing length mismatch which occurs when filtering
            # empty DataFrames or when metadata doesn't match actual data dimensions
            if "Item wrong length" in str(e):
                empty_df = pd.DataFrame(columns=self._data.columns)
                return empty_df
            raise

    def is_column_sorted_within(self, group, column):
        return (
            False
            not in np.concatenate(
                self._data.groupby(group, sort=False)[column]
                .apply(
                    lambda partition: sorted(partition.sort_index().values)
                    == partition.sort_index().values
                )
                .compute()
                .values
            )
            .ravel()
            .tolist()
        )

    def __setitem__(self, key, value):
        if isinstance(value, list):
            chunks = self._data.map_partitions(lambda x: len(x)).compute().to_numpy()
            array_values = da.from_array(value, chunks=tuple(chunks))
            self._data[key] = array_values
        elif isinstance(value, pd.Series):
            self._data = self._data.reset_index()
            self._data = self._data.set_index("index")
            self._data[key] = value
        elif isinstance(value, dd.DataFrame):
            for column in value:
                self._data[column] = value[column]
        else:
            self._data[key] = value

    def __len__(self):
        if not self.length:
            length = self._data.shape[0]
            if not isinstance(length, int):
                length = length.compute()
            self.length = length

        return self.length

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        dataframe = dd.from_dict(data, npartitions=DEFAULT_NUM_PARTITIONS, **kwargs)
        return cls(dataframe)

    @classmethod
    def from_records(cls, data: List[dict], **kwargs):
        data = pd.DataFrame.from_records(data, **kwargs)
        dataframe = dd.from_pandas(data, npartitions=DEFAULT_NUM_PARTITIONS)
        return cls(dataframe)

    @classmethod
    def get_series_values(cls, series) -> list:
        if not cls.is_series(series):
            return []
        if isinstance(cls, pd.Series):
            return series.values
        else:
            return series.compute().values

    def get(self, target: Union[str, List[str]], default=None):
        if isinstance(target, list):
            for column in target:
                if column not in self._data:
                    # List contains values not in the dataset, treat as list of values
                    return default
            return self._data[target].compute()
        elif target in self._data:
            return self._data[target].compute()
        return default

    def convert_to_series(self, result):
        if self.is_series(result):
            if isinstance(result, dd.Series):
                result = result.compute()
            if pd.api.types.is_bool_dtype(result.dtype):
                return result.astype("bool")
            return result
        series = pd.Series(result)
        if pd.api.types.is_bool_dtype(series.dtype):
            return series.astype("bool")
        return series

    def apply(self, func, **kwargs):
        return self._data.apply(func, **kwargs).compute()

    def merge(self, other, **kwargs):
        if isinstance(other, pd.Series):
            new_data = self._data.merge(
                dd.from_pandas(other.reset_index(), npartitions=self._data.npartitions),
                **kwargs,
            )
        else:
            new_data = self._data.merge(other, **kwargs)
        return self.__class__(new_data)

    def __concat_columns(self, current, other):
        for column in other.columns:
            current[column] = other[column]
        return current

    def concat(self, other, **kwargs):
        if kwargs.get("axis", 0) == 1:
            if isinstance(other, list):
                new_data = self._data.copy()
                for dataset in other:
                    new_data = self.__concat_columns(new_data, dataset)
            else:
                new_data = self.__concat_columns(self._data.copy(), other)
        else:
            if isinstance(other, list):
                datasets = [dataset.data for dataset in other]
                new_data = dd.concat([self._data] + datasets, **kwargs)
            else:
                new_data = dd.concat([self._data.copy(), other.data], **kwargs)

        return self.__class__(new_data)

    def groupby(self, by: List[str], **kwargs):
        invalid_kwargs = ["as_index"]
        return self.__class__(
            self._data.groupby(
                by, **self._remove_invalid_kwargs(invalid_kwargs, kwargs)
            )
        )

    def get_grouped_size(self, by, **kwargs):
        if isinstance(self._data, pd.DataFrame):
            grouped_data = self._data[by].groupby(by, **kwargs)
        else:
            grouped_data = self._data[by].compute().groupby(by, **kwargs)
        group_sizes = grouped_data.size()
        if self.is_series(group_sizes):
            group_sizes = group_sizes.to_frame(name="size")

        return group_sizes

    @classmethod
    def is_series(cls, data) -> bool:
        return isinstance(data, dd.Series) or isinstance(data, pd.Series)

    def len(self) -> int:
        return self._data.shape[0].compute()

    def rename(self, index=None, columns=None, inplace=True):
        self._data = self._data.rename(index=index, columns=columns)
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

    def assign(self, **kwargs):
        return self.data.assign(**kwargs)

    def copy(self):
        new_data = self._data.copy()
        return self.__class__(new_data)

    def equals(self, other_dataset):
        is_equal = True
        for column in self.data:
            if column not in other_dataset:
                return False
            is_equal = (
                is_equal
                and self[column]
                .reset_index(drop=True)
                .eq(other_dataset[column].reset_index(drop=True))
                .all()
            )
        return is_equal

    def get_error_rows(self, results) -> "pd.Dataframe":
        """
        Returns a pandas dataframe with all errors found in the dataset. Limited to 1000
        """
        self.data["computed_index"] = 1
        self.data["computed_index"] = self.data["computed_index"].cumsum() - 1
        data_with_results = self.data.set_index("computed_index", sorted=True)
        data_with_results["results"] = results
        data_with_results = data_with_results.fillna(value={"results": False})
        return data_with_results[data_with_results["results"]].head(
            1000, npartitions=-1
        )

    @classmethod
    def cartesian_product(cls, left, right):
        """
        Return the cartesian product of two dataframes
        """
        return cls(
            dd.from_pandas(
                left.compute().merge(right, how="cross"),
                npartitions=DEFAULT_NUM_PARTITIONS,
            )
        )

    def dropna(self, inplace=False, **kwargs):
        result = self._data.dropna(**kwargs)
        if inplace:
            self._data = result
            return None
        else:
            return self.__class__(result)

    def at(self, row_label, col_label):
        """
        Get a single value for a row/column pair.
        """
        partition_index = self.data.loc[row_label:row_label].partitions[0].compute()
        value = partition_index.at[row_label, col_label]
        return value

    def drop_duplicates(self, subset=None, keep="first", **kwargs):
        """
        Drop duplicate rows from the dataset.
        """
        new_data = self._data.drop_duplicates(subset=subset, keep=keep, **kwargs)
        return self.__class__(new_data)

    def replace(self, to_replace, value, **kwargs):
        self._data = self._data.replace(to_replace, value, **kwargs)
        return self

    def astype(self, dtype, **kwargs):
        self._data = self._data.astype(dtype, **kwargs)
        return self

    def filter(self, **kwargs):
        columns_regex = kwargs.get("regex")
        columns_subset = [
            column for column in self.columns if re.match(columns_regex, column)
        ]
        new_data = self._data[columns_subset]
        return self.__class__(new_data)

    def min(self, *args, **kwargs):
        """
        Return the minimum of the values over the requested axis.
        """
        result = self._data.min(*args, **kwargs)
        return self.__class__(result)

    def reset_index(self, drop=False, **kwargs):
        """
        Reset the index of the dataset.
        """
        self._data = self._data.reset_index(drop=drop, **kwargs)
        return self

    def iloc(self, n=None, column=None):
        """
        Purely integer-location based indexing for selection by position.
        """
        if column is None:
            return self._data.iloc[n].compute()
        else:
            return self._data.iloc[n, column].compute()

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
        result = self._data.fillna(value=value, method=method, axis=axis, limit=limit)
        if inplace:
            self._data = result
            return None
        else:
            return self.__class__(result)

    def to_dict(self, **kwargs) -> dict:
        return list(self._data.map_partitions(lambda x: x.to_dict(orient="records")))

    def isin(self, values):
        values_set = set(values)

        def partition_isin(partition):
            return partition.isin(values_set)

        result = self._data.map_partitions(partition_isin)
        return result

    def filter_by_value(self, column, values):
        computed_data = self._data.compute()
        mask = computed_data[column].isin(values)
        filtered_df = computed_data[mask]
        return filtered_df

    def unique(self, column: Optional[str] = None):
        """Return unique values of Series or DataFrame column."""
        if column is not None:
            return self._data[column].unique().compute()
        # If called on series-like data
        if len(self._data.columns) == 1:
            return self._data.iloc[:, 0].unique().compute()
        raise ValueError("Must specify column for DataFrame with multiple columns")

    def squeeze(self, axis=None):
        """Squeeze 1 dimensional axis objects into scalars."""
        result = self._data.compute().squeeze(axis=axis)
        if isinstance(result, (pd.Series, pd.DataFrame)):
            return self.__class__(dd.from_pandas(result, npartitions=DEFAULT_NUM_PARTITIONS))
        return result  # scalar

    def duplicated(self, subset=None, keep='first'):
        """Return boolean Series denoting duplicate rows."""
        # We gotta compute first because dask doesn't support duplicated directly
        return self._data.compute().duplicated(subset=subset, keep=keep)

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
        result = self._data.head(n, npartitions=-1)
        return self.__class__(result)

    def tail(self, n=5):
        """Return the last n rows."""
        result = self._data.tail(n, compute=False)
        return self.__class__(result)

    def nunique(self, axis=0, dropna=True):
        """Count distinct observations."""
        if axis == 0:
            # For each column
            result = {}
            for col in self._data.columns:
                result[col] = self._data[col].nunique(dropna=dropna).compute()
            return pd.Series(result)
        else:
            # Row-wise nunique needs compute
            return self._data.compute().nunique(axis=axis, dropna=dropna)

    def agg(self, func, axis=0, *args, **kwargs):
        """Aggregate using one or more operations."""
        result = self._data.agg(func, axis=axis, *args, **kwargs)
        if isinstance(result, dd.DataFrame):
            return self.__class__(result)
        elif isinstance(result, dd.Series):
            return result.compute()
        return result

    def select_dtypes(self, include=None, exclude=None):
        """Return a subset of the DataFrame's columns based on the column dtypes."""
        # Dask doesn't have select_dtypes, need to compute dtypes first
        dtypes = self._data.dtypes
        columns = []
        
        for col, dtype in dtypes.items():
            if include is not None:
                if isinstance(include, list):
                    if any(dtype == t or pd.api.types.is_dtype_equal(dtype, t) for t in include):
                        columns.append(col)
                else:
                    if dtype == include or pd.api.types.is_dtype_equal(dtype, include):
                        columns.append(col)
            
            if exclude is not None:
                if isinstance(exclude, list):
                    if any(dtype == t or pd.api.types.is_dtype_equal(dtype, t) for t in exclude):
                        if col in columns:
                            columns.remove(col)
                else:
                    if dtype == exclude or pd.api.types.is_dtype_equal(dtype, exclude):
                        if col in columns:
                            columns.remove(col)
        
        if include is None and exclude is not None:
            columns = [col for col in self._data.columns if col not in columns]
        
        return self.__class__(self._data[columns])

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        """Return cumulative sum."""
        result = self._data.cumsum(axis=axis, skipna=skipna)
        return self.__class__(result)

    def to_frame(self, name=None):
        """Convert Series to DataFrame."""
        if isinstance(self._data, dd.Series):
            result = self._data.to_frame(name=name)
            return self.__class__(result)
        return self  # Already a DataFrame

    def describe(self, percentiles=None, include=None, exclude=None):
        """Generate descriptive statistics."""
        result = self._data.describe(percentiles=percentiles, include=include, exclude=exclude)
        return self.__class__(result.compute() if isinstance(result, dd.DataFrame) else result)

    def value_counts(self, normalise=False, sort=True, ascending=False, bins=None, dropna=True):
        """Return a Series containing counts of unique values."""
        if len(self._data.columns) == 1:
            result = self._data.iloc[:, 0].value_counts(
                normalize=normalise, sort=sort, ascending=ascending, bins=bins, dropna=dropna
            )
            return result.compute()
        raise ValueError("value_counts() only works on single columns")

    def shift(self, periods=1, freq=None, axis=0, fill_value=None):
        """Shift index by desired number of periods."""
        result = self._data.shift(periods=periods, freq=freq, axis=axis)
        return self.__class__(result)

    @property
    def shape(self) -> Tuple[int, int]:
        """Return a tuple representing the dimensionality of the dataset."""
        rows = self._data.shape[0]
        if not isinstance(rows, int):
            rows = rows.compute()
        cols = len(self._data.columns)
        return (rows, cols)

    @property
    def dtypes(self):
        """Return the dtypes in the dataset."""
        return self._data.dtypes

    @property
    def values(self):
        """Return a Numpy representation of the data."""
        return self._data.compute().values

    @property
    def str(self):
        """Vectorised string functions for Series and Index."""
        class StringAccessor:
            def __init__(self, data):
                self._data = data
            
            def __getitem__(self, key):
                return self._data[key].str
            
            def __getattr__(self, item):
                if isinstance(self._data, dd.DataFrame):
                    raise AttributeError("Can only use .str accessor with string values!")
                return getattr(self._data.str, item)
        
        return StringAccessor(self._data)

    @property
    def dt(self):
        """Accessor object for datetime-like properties."""
        class DatetimeAccessor:
            def __init__(self, data):
                self._data = data
            
            def __getitem__(self, key):
                return self._data[key].dt
            
            def __getattr__(self, item):
                if isinstance(self._data, dd.DataFrame):
                    raise AttributeError("Can only use .dt accessor with datetimelike values!")
                return getattr(self._data.dt, item)
        
        return DatetimeAccessor(self._data)

    def isin(self, values):
        """Check whether each element is contained in values."""
        values_set = set(values)

        def partition_isin(partition):
            return partition.isin(values_set)

        result = self._data.map_partitions(partition_isin)
        return self.__class__(result)

    def map(self, mapper, na_action=None):
        """Map values using an input mapping or function."""
        if len(self._data.columns) == 1:
            # Map on single column
            result = self._data.iloc[:, 0].map(mapper, na_action=na_action, 
                                              meta=self._data.iloc[:, 0]._meta)
            return result.compute()
        else:
            # For DataFrame, apply map to each column
            result = self._data.map_partitions(
                lambda df: df.apply(lambda x: x.map(mapper, na_action=na_action))
            )
            return self.__class__(result)

    def to_parquet(self, path=None, **kwargs):
        """Write a DataFrame to the parquet format."""
        return self._data.to_parquet(path, **kwargs)

    def eq(self, other, axis='columns', level=None):
        """Get Equal to of dataframe and other, element-wise."""
        result = self._data.eq(other)
        return self.__class__(result)

    def ne(self, other, axis='columns', level=None):
        """Get Not equal to of dataframe and other, element-wise."""
        result = self._data.ne(other)
        return self.__class__(result)

    def lt(self, other, axis='columns', level=None):
        """Get Less than of dataframe and other, element-wise."""
        result = self._data.lt(other)
        return self.__class__(result)

    def le(self, other, axis='columns', level=None):
        """Get Less than or equal to of dataframe and other, element-wise."""
        result = self._data.le(other)
        return self.__class__(result)

    def gt(self, other, axis='columns', level=None):
        """Get Greater than of dataframe and other, element-wise."""
        result = self._data.gt(other)
        return self.__class__(result)

    def ge(self, other, axis='columns', level=None):
        """Get Greater than or equal to of dataframe and other, element-wise."""
        result = self._data.ge(other)
        return self.__class__(result)

    def any(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether any element is True."""
        result = self._data.any(axis=axis, skipna=skipna)
        return result.compute() if hasattr(result, 'compute') else result

    def all(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether all elements are True."""
        result = self._data.all(axis=axis, skipna=skipna)
        return result.compute() if hasattr(result, 'compute') else result

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None, min_count=0, **kwargs):
        """Return the sum of the values."""
        result = self._data.sum(axis=axis, skipna=skipna, min_count=min_count)
        return result.compute() if hasattr(result, 'compute') else result

    def mean(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the mean of the values."""
        result = self._data.mean(axis=axis, skipna=skipna)
        return result.compute() if hasattr(result, 'compute') else result

    def std(self, axis=None, skipna=True, level=None, ddof=1, numeric_only=None, **kwargs):
        """Return sample standard deviation."""
        result = self._data.std(axis=axis, skipna=skipna, ddof=ddof)
        return result.compute() if hasattr(result, 'compute') else result

    def max(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the maximum of the values."""
        result = self._data.max(axis=axis, skipna=skipna)
        return result.compute() if hasattr(result, 'compute') else result

    def round(self, decimals=0, *args, **kwargs):
        """Round to a variable number of decimal places."""
        result = self._data.round(decimals)
        return self.__class__(result)