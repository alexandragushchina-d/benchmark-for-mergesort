
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ParallelMergeSort extends RecursiveAction {
  private Comparable[] array;
  private Comparable[] helper;
  private int low;
  private int high;


  public ParallelMergeSort(Comparable[] array) {
    this.array = array;
    this.helper = new Comparable[array.length];
    this.low = 0;
    this.high = array.length - 1;
  }

  public ParallelMergeSort(Comparable[] array, Comparable[] helper, int low, int high) {
    this.array = array;
    this.helper = helper;
    this.low = low;
    this.high = high;
  }

  @Override
  protected void compute() {
    if (low >= high) return;
    int middle = low + (high - low) / 2;
    ParallelMergeSort left = new ParallelMergeSort(array, helper, low, middle);
    ParallelMergeSort right = new ParallelMergeSort(array, helper, middle + 1, high);
    invokeAll(left, right);
    merge(this.array, this.helper, this.low, middle, this.high);
  }

  private void merge(Comparable[] array, Comparable[] helper, int low, int middle, int high) {
    for (int i = low; i <= high; i++) {
      helper[i] = array[i];
    }
    int i = low;
    int j = middle + 1;
    for (int k = low; k <= high; k++) {
      if (i > middle) {
        array[k] = helper[j++];
      } else if (j > high) {
        array[k] = helper[i++];
      } else if (isLess(helper[i], helper[j])) {
        array[k] = helper[i++];
      } else {
        array[k] = helper[j++];
      }
    }
  }

  private boolean isLess(Comparable a, Comparable b) {
    return a.compareTo(b) < 0;
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Comparable[] array = new Comparable[10];
    array[0] = 245;
    array[1] = 45;
    array[2] = 1;
    array[3] = 10;
    array[4] = 13;
    array[5] = 15;
    array[6] = 24;
    array[7] = 4;
    array[8] = 90;
    array[9] = 1;
    ParallelMergeSort parallelMergeSort = new ParallelMergeSort(array);
    final ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() - 1);
    forkJoinPool.invoke(parallelMergeSort);
    Arrays.stream(array).forEach(item -> System.out.println(item));
  }
}
