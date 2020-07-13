import breeze.linalg.DenseMatrix

//密集矩阵
val a = DenseMatrix((1,2), (3,4))

println(a.cols)
println(a)

//零矩阵
val zero = DenseMatrix.zeros[Int](5,5)

println(a(::,1))

//矩阵转置
val a_t = a.t
