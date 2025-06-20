from abc import ABC, abstractmethod
from typing import Union, List, Any, Optional, Dict, Tuple, Callable


class DatasetInterface(ABC):
    @property
    @abstractmethod
    def data(self):
        """
        Stores the underlying data for the dataset
        """

    @property
    @abstractmethod
    def empty(self):
        """
        Returns whether or not the underlying dataframe is empty
        """

    @property
    @abstractmethod
    def columns(self):
        """
        Stores the columns of the underlying dataset
        """

    @classmethod
    @abstractmethod
    def from_dict(self, data: dict, **kwargs):
        """
        Create the underlying dataset from provided dictionary data
        """

    @classmethod
    @abstractmethod
    def from_records(self, data: List[dict], **kwargs):
        """
        Create the underlying dataset from provided list of records
        """

    @classmethod
    @abstractmethod
    def get_series_values(cls, series) -> list:
        """
        Returns the values for a series.
        """

    @abstractmethod
    def __getitem__(self, item: Union[str, List[str], slice, int]):
        """
        Access dataset column by name
        """

    @abstractmethod
    def __setitem__(self, key: str, data):
        """
        Set value of a dataset column
        """

    @abstractmethod
    def __len__(self):
        """
        Get length of dataset
        """

    @abstractmethod
    def __contains__(self, item: str) -> bool:
        """
        Return true if item is in dataset
        """

    @abstractmethod
    def get(self, column: str, default=None):
        """
        Return column if column is in dataset, else return default
        """

    @abstractmethod
    def groupby(self, by: List[str], **kwargs):
        """
        Group dataframe by list of columns.
        """

    @abstractmethod
    def concat(
        self, other: Union["DatasetInterface", List["DatasetInterface"]], **kwargs
    ):
        """
        Concat two datasets
        """

    @abstractmethod
    def merge(self, other: "DatasetInterface", **kwargs):
        """
        merge two datasets
        """

    @abstractmethod
    def apply(self, func, **kwargs):
        """
        Apply a function to a dataset
        """

    @abstractmethod
    def iterrows(self):
        """
        Return iterator over all dataset rows
        """

    @classmethod
    @abstractmethod
    def is_series(cls, data) -> bool:
        """
        Return true if the data is a series compatible with the underlying dataset
        """

    @abstractmethod
    def convert_to_series(self, data):
        """
        Converts list like data to a series corresponding with the underlying dataset
        """

    @abstractmethod
    def get_series_from_value(self, value):
        """
        Create a series of a single value
        """

    @abstractmethod
    def rename(self, index=None, columns=None, inplace=True):
        """
        Rename columns or index labels.
        """

    @abstractmethod
    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """
        Drop specified labels from rows or columns.
        """

    @abstractmethod
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

    @abstractmethod
    def set_index(self, keys, **kwargs):
        """
        Wrapper for DataFrame set_index method
        """

    @abstractmethod
    def filter(self, **kwargs):
        """
        Wrapper for DataFrame filter method
        """

    @abstractmethod
    def len(self) -> int:
        """
        Return the length of the dataset
        """

    @abstractmethod
    def copy(self) -> "DatasetInterface":
        """
        Return a new instance of the dataset with the same data
        """

    @abstractmethod
    def get_error_rows(self, results):
        """
        Returns a pandas dataframe with all errors found in the dataset. Limited to 1000
        """

    @abstractmethod
    def equals(self, other) -> bool:
        """
        Determine if two datasets are equal
        """

    @abstractmethod
    def where(cond, other, **kwargs):
        """
        Wrapper for dataframe where function
        """

    @abstractmethod
    def cartesian_product(cls, left, right):
        """
        Return the cartesian product of two dataframes
        """

    @abstractmethod
    def sort_values(self, by, **kwargs):
        """
        Sort the dataframe by the provided columns
        """

    @abstractmethod
    def is_column_sorted_within(self, group, column):
        """
        Returns true if the column is sorted within each grouping otherwise false
        """

    @abstractmethod
    def min(self, *args, **kwargs):
        """
        Return the minimum of the values over the requested axis.
        """

    @abstractmethod
    def reset_index(self, drop=False, **kwargs):
        """
        Reset the index of the dataset.
        """

    @abstractmethod
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

    @abstractmethod
    def get_grouped_size(self, by, **kwargs):
        """
        Returns a dataframe containing the sizes of each group in
        the dataframe.
        """

    @abstractmethod
    def to_dict(self, **kwargs) -> dict:
        """
        Convert the dataset to a dictionary.
        """

    @abstractmethod
    def unique(self, column: Optional[str] = None):
        """
        Return unique values of Series or DataFrame column.
        If column is None and called on Series-like data, return unique values.
        """
        pass

    @abstractmethod
    def squeeze(self, axis=None):
        """
        Squeeze 1 dimensional axis objects into scalars.
        """
        pass

    @abstractmethod
    def duplicated(self, subset=None, keep='first'):
        """
        Return boolean Series denoting duplicate rows.
        """
        pass

    @abstractmethod
    def isna(self):
        """
        Detect missing values.
        """
        pass

    @abstractmethod
    def notna(self):
        """
        Detect non-missing values.
        """
        pass

    @abstractmethod
    def head(self, n=5):
        """
        Return the first n rows.
        """
        pass

    @abstractmethod
    def tail(self, n=5):
        """
        Return the last n rows.
        """
        pass

    @abstractmethod
    def nunique(self, axis=0, dropna=True):
        """
        Count distinct observations.
        """
        pass

    @abstractmethod
    def agg(self, func, axis=0, *args, **kwargs):
        """
        Aggregate using one or more operations.
        """
        pass

    @abstractmethod
    def select_dtypes(self, include=None, exclude=None):
        """
        Return a subset of the DataFrame's columns based on the column dtypes.
        """
        pass

    @abstractmethod
    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        """
        Return cumulative sum.
        """
        pass

    @abstractmethod
    def to_frame(self, name=None):
        """
        Convert Series to DataFrame.
        """
        pass

    @abstractmethod
    def describe(self, percentiles=None, include=None, exclude=None):
        """
        Generate descriptive statistics.
        """
        pass

    @abstractmethod
    def value_counts(self, normalise=False, sort=True, ascending=False, bins=None, dropna=True):
        """
        Return a Series containing counts of unique values.
        """
        pass

    @abstractmethod
    def shift(self, periods=1, freq=None, axis=0, fill_value=None):
        """
        Shift index by desired number of periods.
        """
        pass

    @property
    @abstractmethod
    def shape(self) -> Tuple[int, int]:
        """Return a tuple representing the dimensionality of the dataset."""
        pass

    @property
    @abstractmethod
    def dtypes(self):
        """Return the dtypes in the dataset."""
        pass

    @property
    @abstractmethod
    def values(self):
        """Return a representation of the data."""
        pass

    @property
    @abstractmethod
    def str(self):
        """Vectorised string functions for Series and Index."""
        pass

    @property
    @abstractmethod
    def dt(self):
        """Accessor object for datetime-like properties."""
        pass

    @abstractmethod
    def isin(self, values):
        """Check whether each element is contained in values."""
        pass

    @abstractmethod
    def at(self, row_label, col_label):
        """Access a single value for a row/column label pair."""
        pass

    @abstractmethod
    def iloc(self, row_indexer, col_indexer=None):
        """Purely integer-location based indexing for selection by position."""
        pass

    @abstractmethod
    def map(self, mapper, na_action=None):
        """Map values using an input mapping or function."""
        pass

    @abstractmethod
    def to_parquet(self, path=None, **kwargs):
        """Write a DataFrame to the parquet format."""
        pass

    @abstractmethod
    def assign(self, **kwargs):
        """Assign new columns to a DataFrame."""
        pass

    @abstractmethod
    def eq(self, other, axis='columns', level=None):
        """Get Equal to of dataframe and other, element-wise."""
        pass

    @abstractmethod
    def ne(self, other, axis='columns', level=None):
        """Get Not equal to of dataframe and other, element-wise."""
        pass

    @abstractmethod
    def lt(self, other, axis='columns', level=None):
        """Get Less than of dataframe and other, element-wise."""
        pass

    @abstractmethod
    def le(self, other, axis='columns', level=None):
        """Get Less than or equal to of dataframe and other, element-wise."""
        pass

    @abstractmethod
    def gt(self, other, axis='columns', level=None):
        """Get Greater than of dataframe and other, element-wise."""
        pass

    @abstractmethod
    def ge(self, other, axis='columns', level=None):
        """Get Greater than or equal to of dataframe and other, element-wise."""
        pass

    @abstractmethod
    def any(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether any element is True."""
        pass

    @abstractmethod
    def all(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether all elements are True."""
        pass

    @abstractmethod
    def sum(self, axis=None, skipna=True, level=None, numeric_only=None, min_count=0, **kwargs):
        """Return the sum of the values."""
        pass

    @abstractmethod
    def mean(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the mean of the values."""
        pass

    @abstractmethod
    def std(self, axis=None, skipna=True, level=None, ddof=1, numeric_only=None, **kwargs):
        """Return sample standard deviation."""
        pass

    @abstractmethod
    def max(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the maximum of the values."""
        pass

    @abstractmethod
    def round(self, decimals=0, *args, **kwargs):
        """Round to a variable number of decimal places."""
        pass