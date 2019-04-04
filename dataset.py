import csv, os
from operator import itemgetter


class Dataset(object):
    """This object is used to represent the data in the given CSVs and to perform filtering/aggregations on."""

    def __init__(self, name, columns=None, data=None):
        self.name = name
        self.columns = columns or []
        self.data = data or []

    def populate(self, csv_path, limit=None):
        """Populates this dataset with data from the csv_path

        Args:
             csv_path (str): the path to the csv
             limit(int): if passed, stops processing after limit rows

        Returns:
            Dataset: self
        """
        with open(csv_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for i, row in enumerate(csv_reader):
                if limit and i > limit:
                    break

                if i == 0:
                    self.columns = row
                    continue
                self.data.append(self._parse_row(row))

        return self

    def _parse_row(self, row):
        """Parses a row from a CSV and converts numbers to floats

        Args:
            row (iterable): a row of data in a csv

        Returns:
            tuple: the new row, after converting types
        """
        element = []
        for i, cell in enumerate(row):
            try:
                cell = float(cell)
            except ValueError:
                pass
            element.append(cell)
        return tuple(element)

    def _validate_column_name(self, column_name):
        if column_name not in self.columns:
            raise ValueError('column name {} does not exist'.format(column_name))

    def get_column(self, column_name, row):
        """Gets a column value from a row in this dataset

        Args:
            column_name (str): the column name to select from a row
            row (tuple): the row in this dataset to select from

        Returns:
            object: the selected element
        """
        self._validate_column_name(column_name)
        return row[self.columns.index(column_name)]

    def equals_filter(self, column_name, value):
        return self.filter(column_name, lambda x: x == value)

    def between_filter(self, column_name, start, end):
        return self.filter(column_name, lambda x: start < x < end)

    def filter(self, column_name, function):
        """Removes data from this dataset based on if the given column passed the
        conditions defined by the function argument

        Args:
            column_name (str): the column to pass to function
            function (callable): a function where if it returns false, the current row is filtered

        Returns:
            Dataset: this dataset after filtering
        """
        self._validate_column_name(column_name)
        new_data = []
        for row in self.data:
            if function(self.get_column(column_name, row)):
                new_data.append(row)

        return Dataset(self.name, columns=self.columns, data=new_data)

    def _partition(self, group_by_columns, aggregate_column=None):
        """splits the data in this dataset into partitions based on the passed group_by_columns,
        where rows that contain like values for the given group_by_columns will be grouped together

        Args:
            group_by_columns (iterable of str): the columns to split the data by
            aggregate_column (str): if passed, reduces the data further to just this column

        Returns:
            dict of tuple, list: the partitioned data, where the keys are the combinations of the group
                by columns that exist in this dataset, and the values are the rows that contain the values
                in the key
        """
        partitioned_data = {}
        for row in self.data:
            key = []
            for column_name in group_by_columns:
                key.append(self.get_column(column_name, row))
            key = tuple(key)

            if aggregate_column:
                row = self.get_column(aggregate_column, row)

            if key in partitioned_data:
                partitioned_data[key].append(row)
            else:
                partitioned_data[key] = [row]

        return partitioned_data

    def group_by_and_aggregate(self, group_by_columns, aggregate_column, aggregation):
        """Groups the data in this dataset by the group_by_columns, and then executes the given aggregation
        on the given aggregate_column

        Args:
            group_by_columns (iterable of str): the columns in this dataset to partition the data by
            aggregate_column (str): the column to execute the given aggregation against
            aggregation (callable): a function to execute on a list of the values for the given aggregate
                column

        Returns:
            Dataset: The resulting dataset, where the resulting columns are only the group_by_columns and the aggregate
                column
        """
        for column_name in list(group_by_columns) + [aggregate_column]:
            self._validate_column_name(column_name)

        partitioned_data = self._partition(group_by_columns, aggregate_column=aggregate_column)

        new_data = []
        new_columns = list(group_by_columns) + [aggregate_column]
        for key, rows in partitioned_data.items():
            result = aggregation(rows)
            element = list(key) + [result]
            element = tuple(element)
            new_data.append(element)

        return Dataset(self.name, columns=new_columns, data=new_data)

    def sort_by(self, column_name, reverse=False):
        """Sorts the data in this dataset by the passed column_name in ascending order by default

        Args:
            column_name (str): the column to sort the data by
            reverse (bool): if true, sorts the data in descending order

        Returns:
            Dataset: the sorted dataset
        """
        self._validate_column_name(column_name)
        new_data = sorted(self.data, key=itemgetter(self.columns.index(column_name)), reverse=reverse)
        return Dataset(self.name, columns=self.columns, data=new_data)

    def limit(self, n):
        return Dataset(self.name, columns=self.columns, data=self.data[:n])

    def inner_join(self, other_dataset, join_column):
        """Performs an inner join, where the column names are assumed to be unique except
        for the join_column.

        Args:
            other_dataset (Dataset): an external dataset to join on
            join_column (str): a common column between these two datasets to join on

        Returns:
            Dataset: the joined dataset
        """
        if join_column not in self.columns or join_column not in other_dataset.columns:
            raise ValueError('join_column {} does not exist in both datasets'.format(join_column))

        indexed_data = other_dataset._partition((join_column,))

        # create a new instance of this list to avoid modifying the other_dataset argument by reference
        their_columns = list(other_dataset.columns)
        their_columns.remove(join_column)
        new_columns = self.columns + their_columns

        new_data = []
        for row in self.data:
            # convert this string to a tuple
            key = (self.get_column(join_column, row),)
            if key in indexed_data:
                their_data = indexed_data[key]
                # handle cases where there are duplicate values to join on
                for their_row in their_data:
                    new_row = list(row)
                    # append each external column onto this dataset
                    for their_column in their_columns:
                        their_column_value = other_dataset.get_column(their_column, their_row)
                        new_row.append(their_column_value)
                    new_row = tuple(new_row)
                    new_data.append(new_row)

        return Dataset(self.name, columns=new_columns, data=new_data)

    def __repr__(self):
        return str((self.name, self.columns, self.data))

if __name__ == '__main__':
    product_report = Dataset('product_report')
    product_report.populate(os.path.join('data', 'product_report.csv'))

    # Get the best selling item in the 30 days prior to 6/1/2017, and the total sales for that item.
    answer = Dataset('ad_report')\
        .populate(os.path.join('data', 'ad_report.csv'))\
        .between_filter('date', '2017-05-01', '2017-06-01')\
        .group_by_and_aggregate(('asin',), 'sales', sum)\
        .sort_by('sales', reverse=True)\
        .limit(1)\
        .inner_join(product_report, 'asin')

    print(answer)
