// package com.example.spark.udaf

// import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.Row

// class ModeFrequencyUDAF extends UserDefinedAggregateFunction {

//   // Input schema (one input column of type String)
//   override def inputSchema: StructType = StructType(Array(
//     StructField("value", StringType)
//   ))

//   // Buffer schema (struct format: two columns, value and frequency)
//   override def bufferSchema: StructType = StructType(Array(
//     StructField("mode", StringType),      // Current mode
//     StructField("frequency", IntegerType) // Frequency of the current mode
//   ))

//   // Output schema (struct format: mode and frequency)
//   override def dataType: DataType = StructType(Array(
//     StructField("mode", StringType),
//     StructField("frequency", IntegerType)
//   ))

//   // Whether the function is deterministic
//   override def deterministic: Boolean = true

//   // Initialize the buffer
//   override def initialize(buffer: MutableAggregationBuffer): Unit = {
//     buffer(0) = null  // Mode is initially null
//     buffer(1) = 0     // Frequency is initially 0
//   }

//   // Update the buffer with a new input row
//   override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//     val currentMode = buffer.getString(0)
//     val currentFrequency = buffer.getInt(1)
//     val newValue = input.getString(0)

//     if (currentMode == null || currentMode == newValue) {
//       // Either the mode is null (first value) or matches the current mode
//       buffer(0) = newValue
//       buffer(1) = currentFrequency + 1
//     } else if (currentFrequency == 0) {
//       // No current mode yet, set the new value as mode
//       buffer(0) = newValue
//       buffer(1) = 1
//     }
//   }

//   // Merge two buffers
//   override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//     val mode1 = buffer1.getString(0)
//     val frequency1 = buffer1.getInt(1)

//     val mode2 = buffer2.getString(0)
//     val frequency2 = buffer2.getInt(1)

//     if (frequency2 > frequency1) {
//       // If buffer2 has a mode with higher frequency, use it
//       buffer1(0) = mode2
//       buffer1(1) = frequency2
//     } else if (frequency2 == frequency1 && mode2 != null) {
//       // In case of a tie, prefer the second buffer's mode (arbitrary choice)
//       buffer1(0) = mode2
//     }
//   }

//   // Evaluate the final result
//   override def evaluate(buffer: Row): Any = {
//     // Return the mode and its frequency as a Struct
//     Row(buffer.getString(0), buffer.getInt(1))
//   }
// }
/////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.example.spark.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class ModeFrequencyUDAF extends UserDefinedAggregateFunction {

  // Input schema (one input column of type String)
  override def inputSchema: StructType = StructType(Array(
    StructField("value", StringType)
  ))

  // Buffer schema (struct format: three columns - mode, frequency, value count map)
  override def bufferSchema: StructType = StructType(Array(
    StructField("mode", StringType),            // Current mode
    StructField("frequency", IntegerType),      // Frequency of the current mode
    StructField("valueCountMap", MapType(StringType, IntegerType)) // Tracking count of all values
  ))

  // Output schema (struct format: mode and frequency)
  override def dataType: DataType = StructType(Array(
    StructField("mode", StringType),
    StructField("frequency", IntegerType)
  ))

  // Whether the function is deterministic
  override def deterministic: Boolean = true

  // Initialize the buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null    // Mode is initially null
    buffer(1) = 0       // Frequency is initially 0
    buffer(2) = Map.empty[String, Int] // Empty count map
  }

  // Update the buffer with a new input row
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newValue = input.getString(0)
    
    // Get or initialize the value count map
    val currentValueCountMap = buffer.getMap[String, Int](2)
    
    // Update the count map
    val updatedValueCountMap = currentValueCountMap + 
      (newValue -> (currentValueCountMap.getOrElse(newValue, 0) + 1))
    
    // Find the mode based on max frequency
    val newMode = updatedValueCountMap.maxBy(_._2)._1
    val newFrequency = updatedValueCountMap(newMode)
    
    // Update buffer
    buffer(0) = newMode
    buffer(1) = newFrequency
    buffer(2) = updatedValueCountMap
  }

  // Merge two buffers
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val mode1 = buffer1.getString(0)
    val frequency1 = buffer1.getInt(1)
    val valueCountMap1 = buffer1.getMap[String, Int](2)

    val mode2 = buffer2.getString(0)
    val frequency2 = buffer2.getInt(1)
    val valueCountMap2 = buffer2.getMap[String, Int](2)

    // Handle case of empty maps
    if (valueCountMap1.isEmpty && valueCountMap2.isEmpty) {
      buffer1(0) = null
      buffer1(1) = 0
      buffer1(2) = Map.empty[String, Int]
      return
    }

    // Merge the value count maps
    val mergedValueCountMap = (valueCountMap1.keys ++ valueCountMap2.keys)
      .map(key => key -> (valueCountMap1.getOrElse(key, 0) + valueCountMap2.getOrElse(key, 0)))
      .toMap

    // Find the new mode based on max frequency
    val newMode = mergedValueCountMap.maxBy(_._2)._1
    val newFrequency = mergedValueCountMap(newMode)

    // Update buffer
    buffer1(0) = newMode
    buffer1(1) = newFrequency
    buffer1(2) = mergedValueCountMap
  }

  // Evaluate the final result
  override def evaluate(buffer: Row): Any = {
    // Return the mode and its frequency as a Struct
    Row(buffer.getString(0), buffer.getInt(1))
  }
}

















