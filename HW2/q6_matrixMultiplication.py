from pyspark.sql import SparkSession
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
import operator

spark = SparkSession.builder.appName('MatrixMultiplication').getOrCreate()
sc=spark.sparkContext

inputMatrix1 = sc.textFile("file:///Users/adhutsav/Desktop/BigData/HW2/input/smallMatrix1.txt")
inputMatrix2 = sc.textFile("file:///Users/adhutsav/Desktop/BigData/HW2/input/smallMatrix2.txt")

def rowSplit(line):
    rowData = line[0].split()
    rowIdx = line[1]
    res = []
    for cIdx, val in enumerate(rowData):
        res.append(str(rowIdx) + " " + str(cIdx) + " " + str(val))
    return res

def to_matrixA(line):
    i, j, val = line.split()
    return (j, (i, val))

def to_matrixB(line):
    j, k, val = line.split()
    return (j, (k, val))

def matrixMultiplication(x):
    left = x[1][0]
    right = x[1][1]
    i, v1 = left
    k, v2 = right
    return ((i, k), (int(v1) * int(v2)))

def to_MatrixEntry(line):
    input = line.split()
    return MatrixEntry(int(input[0]), int(input[1]), float(input[2]))

matrix1_entries = inputMatrix1.zipWithIndex().flatMap(rowSplit)
matrix2_entries = inputMatrix2.zipWithIndex().flatMap(rowSplit)

matrix1 = CoordinateMatrix(matrix1_entries.map(to_MatrixEntry))
matrix2 = CoordinateMatrix(matrix2_entries.map(to_MatrixEntry))

ROW_MAT1, COL_MAT1 = matrix1.numRows(), matrix1.numCols()
ROW_MAT2, COL_MAT2 = matrix2.numRows(), matrix2.numCols()

if COL_MAT1 != ROW_MAT2:
    print("Cannot do Matrix Multiplication: {ROW_MAT1} * {COL_MAT1} AND {ROW_MAT2} * {COL_MAT2} ")
    exit(2)

print(f"Matrix1 Dimensions : {ROW_MAT1} * {COL_MAT1}")
print(f"Matrix2 Dimensions : {ROW_MAT2} * {COL_MAT2}")

matrix1_entries = matrix1_entries.map(to_matrixA)
matrix2_entries = matrix2_entries.map(to_matrixB)

matA_matB = matrix1_entries.join(matrix2_entries).map(matrixMultiplication)\
            .reduceByKey(operator.add).map(lambda x : (x[0][0], x[0][1], x[1]))

results = matA_matB.collect()
resultMatrix = CoordinateMatrix(matA_matB.map(to_MatrixEntry))

with open ("q6_output.txt", "w") as f:
    f.write(f"Matrix1 Dimensions : {ROW_MAT1} * {COL_MAT1}\n")
    f.write(f"Matrix2 Dimensions : {ROW_MAT2} * {COL_MAT2}\n")
    for value in results:
        f.write(f"{value}\n")

