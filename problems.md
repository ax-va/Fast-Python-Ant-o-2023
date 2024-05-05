## CSV problems:
- no typing of columns
- representing numbers is much more compactly in binary form than in text -> the format itself is inefficient
- each line in a CSV can vary in size -> not possible to compute the location of rows and columns -> no efficient jumping

Solution: Apache Parquet to efficiently store heterogeneous tabular data, but with some restrictions from the Java world