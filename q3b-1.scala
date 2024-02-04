//step 1. Simulate a stream
// Create an Array of 1 to 100 Int
var stream = Array.range(1, 101)

// Find and store the number of values in the stream
val N = stream.length

//step 2. Create the reservoir memory
// Set the length of the reservoir
val n = 10


// Book some space for the receiving stream
val reservoir = new Array[Int](n)


//step 3. Perform sampling
val myrandnum = scala.util.Random

// Set a random seed (can be any number)
myrandnum.setSeed(5202)

for (k <- 0 until N) {
 // Store the first k numbers
if (k < n) {
reservoir(k) = stream(k)
}
else {
// Pick a random index.
val t = myrandnum.nextInt(k)
// replace t-th element if t is a valid index
if (t < n) {
 reservoir(t) = stream(k)
 }
 }
}

//step 4.  Print the output.

println("These "+n.toString+" values: ")
println(reservoir.mkString(" "))
println("are randomly selected from these "+N.toString+" values: ")
println(stream.mkString(" "))

System.exit(0)