import java.util.Arrays;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ForkJoinPool;

/**
 * Uses parallel programming to perform merge sort. Works in conjunction with MergeSort.java.
 */
public class ParallelMergeSort {
	public static void main(String[] args) {
		/* Set the size of the two arrays to a big number. */
		final int SIZE = 50000000;
		/* Initialize two arrays with the same size. */
		int[] list1 = new int[SIZE];
		int[] list2 = new int[SIZE];

		/* Fill up the arrays with random integers from 0 to 10000000 */
		for (int i = 0; i < list1.length; i++)
			list1[i] = list2[i] = (int)(Math.random() * 10000000);

		/* We'll try to sort the first array using parallel programming, and get the elapsed time. */
		System.out.printf("Running parallel mergesort (with %s processors) on a list with %s elements...\n",
				Runtime.getRuntime().availableProcessors(), SIZE);
		long startTime = System.currentTimeMillis();
		parallelMergeSort(list1); // Invoke parallel merge sort
		long endTime = System.currentTimeMillis();
		System.out.printf("Finished. Elapsed time: %s milliseconds.\n", endTime - startTime);

		/* For comparison purposes, we'll sort the second array using normal
		 * sequential method, and get the elapsed time. */
		System.out.println("\nNow running normal (sequential) mergesort on a list of equal size...");
		startTime = System.currentTimeMillis();
		MergeSort.mergeSort(list2); // MergeSort.java
		endTime = System.currentTimeMillis();
		System.out.printf("Finished. Elapsed time: %s milliseconds.", endTime - startTime);
	}

	/**
	 * Method: parallelMergeSort
	 * Uses parallel programming to merge-sort given array of ints.
	 *
	 * @param list given array of ints
	 */
	public static void parallelMergeSort(int[] list) {
		/* ForkJoinTask is the abstract base class for tasks. The Fork/Join Framework is designed
		 * to parallelize divide-and-conquer solutions, which are naturally recursive.
		 * RecursiveAction and RecursiveTask are two subclasses of ForkJoinTask.
		 * To define a concrete task class, your class should extend RecursiveAction
		 * or RecursiveTask. RecursiveAction is for a task that doesn’t return a value, and
		 * RecursiveTask is for a task that does return a value. Your task class should override the
		 * compute() method to specify how a task is performed.
		 * We use RecursiveAction here since mergesort doesn't need to return a value. */
		RecursiveAction mainTask = new SortTask(list);  //SortTask is defined below.

		/* The ForkJoinPool class executes Fork/Join tasks. */
		ForkJoinPool pool = new ForkJoinPool();

		/* Execute the task */
		pool.invoke(mainTask);
	}

	/**
	 * Nested class that extends RecursiveAction.
	 */
	private static class SortTask extends RecursiveAction {

		private static final long serialVersionUID = 1L;

		/* This is the threshold for the size of the array. If the size of the array is smaller than
		 * this number, then the sorting will just be done sequentially.
		 * If the size of the array is larger than this number, then the sorting will be done
		 * using parallel programming. */
		private final int THRESHOLD = 500;
		private int[] list;

		/**
		 * 1-arg constructor.
		 * @param list given array of ints
		 */
		SortTask(int[] list) {
			this.list = list;
		}

		/**
		 * Method: compute. Overrides built-in method.
		 * Sorts the array of ints using parallel programming.
		 */
		@Override
		protected void compute() {
			/* If the list size is pretty small, then just use built-in Arrays.sort() method. */
			if (list.length < THRESHOLD)
				Arrays.sort(list);
			/* Otherwise, if the list size is pretty big... */
			else {
				// Obtain the first half
				int[] firstHalf = new int[list.length / 2];	//initialize an array to hold the first half
				System.arraycopy(list, 0, firstHalf, 0, list.length / 2);	//copy the first half the array

				// Obtain the second half
				int secondHalfLength = list.length - list.length / 2;
				int[] secondHalf = new int[secondHalfLength];	//initialize an array to hold the 2nd half
				System.arraycopy(list, list.length / 2, secondHalf, 0, secondHalfLength);	//copy the 2nd half

				// Recursively sort the two halves.
				invokeAll(new SortTask(firstHalf), new SortTask(secondHalf));

				// ALT (longer way for the previous line of code):
//				RecursiveAction first = new SortTask(firstHalf);	//initialize recursiveaction classes
//				RecursiveAction second = new SortTask(secondHalf);
//				first.fork();	//Recursively execute the tasks
//				second.fork();
//				first.join();	//After the tasks are recursively executed, we need to join the parallel threads.
//				second.join();

				/* Use MergeSort.java method to merge firstHalf with secondHalf into a single arraylist.
				 * This method mutates this.list so that it's sorted. */
				MergeSort.merge(firstHalf, secondHalf, this.list);  //See MergeSort.java
			}
		}
	}
}