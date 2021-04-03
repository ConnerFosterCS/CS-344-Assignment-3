use std::env; // to get arugments passed to the program
use std::thread;

/*
* Print the number of partitions and the size of each partition
* @param vs A vector of vectors
*/
fn print_partition_info(vs: &Vec<Vec<usize>>){
    println!("Number of partitions = {}", vs.len());
    for i in 0..vs.len(){
        println!("\tsize of partition {} = {}", i, vs[i].len());
    }
}

/*
* Create a vector with integers from 0 to num_elements -1
* @param num_elements How many integers to generate
* @return A vector with integers from 0 to (num_elements - 1)
*/
fn generate_data(num_elements: usize) -> Vec<usize>{
    let mut v : Vec<usize> = Vec::new();
    for i in 0..num_elements {
        v.push(i);
    }
    return v;
}

/*
* Partition the data in the vector v into 2 vectors
* @param v Vector of integers
* @return A vector that contains 2 vectors of integers
*/
fn partition_data_in_two(v: &Vec<usize>) -> Vec<Vec<usize>>{
    let partition_size = v.len() / 2;
    // Create a vector that will contain vectors of integers
    let mut xs: Vec<Vec<usize>> = Vec::new();

    // Create the first vector of integers
    let mut x1 : Vec<usize> = Vec::new();
    // Add the first half of the integers in the input vector to x1
    for i in 0..partition_size{
        x1.push(v[i]);
    }
    // Add x1 to the vector that will be returned by this function
    xs.push(x1);

    // Create the second vector of integers
    let mut x2 : Vec<usize> = Vec::new();
    // Add the second half of the integers in the input vector to x2
    for i in partition_size..v.len(){
        x2.push(v[i]);
    }
    // Add x2 to the vector that will be returned by this function
    xs.push(x2);
    // Return the result vector
    xs
}

/*
* Sum up the all the integers in the given vector
* @param v Vector of integers
* @return Sum of integers in v
* Note: this function has the same code as the reduce_data function.
*       But don't change the code of map_data or reduce_data.
*/
fn map_data(v: &Vec<usize>) -> usize{
    let mut sum = 0;
    for i in v{
        sum += i;
    }
    sum
}

/*
* Sum up the all the integers in the given vector
* @param v Vector of integers
* @return Sum of integers in v
*/
fn reduce_data(v: &Vec<usize>) -> usize{
    let mut sum = 0;
    for i in v{
        sum += i;
    }
    sum
}

/*
* A single threaded map-reduce program
*/
fn main() 
{
    // Use std::env to get arguments passed to the program
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("ERROR: Usage {} num_partitions num_elements", args[0]);
        return;
    }
    let num_partitions : usize = args[1].parse().unwrap();
    let num_elements : usize = args[2].parse().unwrap();
    if num_partitions < 1{
      println!("ERROR: num_partitions must be at least 1");
        return;
    }
    if num_elements < num_partitions{
        println!("ERROR: num_elements cannot be smaller than num_partitions");
        return;
    }

    // Generate data.
    let v = generate_data(num_elements);

    // PARTITION STEP: partition the data into 2 partitions
    let xs = partition_data_in_two(&v);

    // Print info about the partitions
    print_partition_info(&xs);

    let mut intermediate_sums : Vec<usize> = Vec::new();

    // MAP STEP: Process each partition

    // CHANGE CODE START: Don't change any code above this line

    // Change the following code to create 2 threads that run concurrently and each of which uses map_data() function to process one of the two partitions

    let clone1 = xs[0].clone();
    let thread1 = thread::spawn(move || {map_data(&clone1)});
    let clone2 = xs[1].clone();
    let thread2 = thread::spawn(move || {map_data(&clone2)});
    let join1 = thread1.join().unwrap();
    let join2 = thread2.join().unwrap();
    intermediate_sums.push(join1);
    intermediate_sums.push(join2);

    // CHANGE CODE END: Don't change any code below this line until the next CHANGE CODE comment

    // Print the vector with the intermediate sums
    println!("Intermediate sums = {:?}", intermediate_sums);

    // REDUCE STEP: Process the intermediate result to produce the final result
    let sum = reduce_data(&intermediate_sums);
    println!("Sum = {}", sum);


    // CHANGE CODE: Add code that does the following:
	let mut new_sum : Vec<usize> = Vec::new();
	let mut n_thread = Vec::new(); let mut r_thread = Vec::new();
	let mut looper = 0; //controlls the flow of my loop that creates and processes the partitions
    // 1. Calls partition_data to partition the data into equal partitions
	let partition = partition_data(num_partitions, &v);
    // 2. Calls print_partiion_info to print info on the partitions that have been created
	print_partition_info(&partition);
    // 3. Creates one thread per partition and uses each thread to process one partition
	loop 
	{
		let partition_clone = partition.clone();
        n_thread.push(thread::spawn(move || map_data(&partition_clone[looper])));
		looper = looper + 1; //looper is incremented before the break check to not reach too far into the array
		if looper == num_partitions //if all threads are created
		{ 
			for i in n_thread { r_thread.push(i.join().unwrap()); } //processes each partition after they have been created.
			break; //after the data has been processed, end the loops
		}
	}
	// 4. Collects the intermediate sums from all the threads
    for i in 0..num_partitions { new_sum.push(r_thread[i]); }
    // 5. Prints information about the intermediate sums
	println!("Intermediate sums = {:?}", new_sum);
    // 6. Calls reduce_data to process the intermediate sums
	let reduce_sum = reduce_data(&new_sum);
    // 7. Prints the final sum computed by reduce_data
	println!("Sum = {}", reduce_sum);
}

/*
* CHANGE CODE: code this function
* Note: Don't change the signature of this function
*
* Partitions the data into a number of partitions such that
*   - the returned partitions contain all elements that are in the input vector
*   - all partitions (except possibly the the last one) have equal number 
*      of elements. The last partition may have either the same number of
*      elements or fewer elements than the other partitions.
* UPDATE AUGUST 10: Please see Piazza post @209 about another choice of how to partition the data
*
* @param num_partitions The number of partitions to create
* @param v The data to be partitioned
* @return A vector that contains vectors of integers
* 
*/
fn partition_data(num_partitions: usize, v: &Vec<usize>) -> Vec<Vec<usize>>
{
	let mut xs: Vec<Vec<usize>> = Vec::new(); // Create a vector that will contain vectors of integers
	let mut count = 0; //variable used to control loops. generally for the length of the number of partitions
	let mut looper = 0; //used to increment the for loop range that pushes the vector.
	let value = v.len()/num_partitions;

	if v.len()%num_partitions == 0 //if the number is evenly divisable (remainder = 0)...
	{	    
		while count < num_partitions //generate data for every partition...
		{
			let mut x1 : Vec<usize> = Vec::new(); // Create the vector of integers

			for i in looper..looper + value //The loops start and end (range) need to be incremented by the number of partitions 
			{
				x1.push(v[i]); //Add the array at i to x1
			}
			xs.push(x1); // Add x1 to the vector that will be returned by this function
			count = count + 1; //increment count to continue this loop
			looper = looper + value; //increment the for loop range by the number of partitions
		}
	}
	else //if there is a remainder (if it is not evenly divisable)...
	{	
		let remainder = v.len()%num_partitions; //Get the remainder value to determine how many numbers need + 1 to them to add up to the proper number

		while count < num_partitions //generate data for every partition...
		{
			let mut x1 : Vec<usize> = Vec::new(); // Create the first vector of integers

			for i in looper..looper + value //The loops start and end (range) need to be incremented by the number of partitions
			{
				x1.push(v[i]); //Add the array at i to x1
			}

			if count < remainder //if this number needs +1 because of the remainder
			{
				x1.push(v[looper + value]); //Add the array at i to x1
				looper = looper + 1; //here you increment the looper by one for the numbers that need + 1 so the range of the previous for loop is proper
			}
			
			xs.push(x1); // Add x1 to the vector that will be returned by this function
			count = count + 1; //increment count to continue this loop
			looper = looper + value; //increment the for loop range by the number of partitions
		}
	}
	xs //return the new values
}