
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class BetterParallelMergeSort extends RecursiveAction {
  private Comparable[] array;
  private Comparable[] helper;
  private int low;
  private int high;
  private int depth = 0;
  private static final int minArraySize = 10;

  public BetterParallelMergeSort(Comparable[] array) {
    this.array = array;
    this.helper = new Comparable[array.length];
    this.low = 0;
    this.high = array.length - 1;
  }

  public BetterParallelMergeSort(Comparable[] array, Comparable[] helper, int low, int high, int depth) {
    this.array = array;
    this.helper = helper;
    this.low = low;
    this.high = high;
    this.depth = depth;
  }

  @Override
  protected void compute() {
    if (high - low <= minArraySize ||
      getPool().getParallelism() <= Math.pow(2, depth)) {
      mergesort(array, helper, low, high);
    } else {
      int middle = low + ((high - low) / 2);
      BetterParallelMergeSort job1 = new BetterParallelMergeSort(array, helper, low, middle, depth + 1);
      BetterParallelMergeSort job2 = new BetterParallelMergeSort(array, helper, middle + 1, high, depth + 1);
      invokeAll(job1, job2);
      merge(array, helper, low, middle, high);
    }

  }

  public void mergesort(final Comparable[] array, final Comparable[] helper, final int low, final int high) {
    if (low < high) {
      final int middle = (low + high) / 2;
      mergesort(array, helper, low, middle);
      mergesort(array, helper, middle + 1, high);
      merge(array, helper, low, middle, high);
    }
  }

  public void merge(final Comparable[] array, final Comparable[] helper, final int low, final int middle, final int high) {
    for (int i = low; i <= high; i++) {
      helper[i] = array[i];
    }

    int helperLeft = low;
    int helperRight = middle + 1;
    int current = low;

    while (helperLeft <= middle && helperRight <= high) {
      if (helper[helperLeft].compareTo(helper[helperRight]) <= 0) {
        array[current] = helper[helperLeft++];
      } else {
        array[current] = helper[helperRight++];
      }
      current++;
    }

    while (helperLeft <= middle) {
      array[current++] = helper[helperLeft++];
    }
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
/*    Comparable[] array = new Comparable[30];
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
    array[10] = 245;
    array[11] = 45;
    array[12] = 1;
    array[13] = 10;
    array[14] = 13;
    array[15] = 15;
    array[16] = 24;
    array[17] = 4;
    array[18] = 90;
    array[19] = 1;
    array[20] = 245;
    array[21] = 45;
    array[22] = 1;
    array[23] = 10;
    array[24] = 13;
    array[25] = 15;
    array[26] = 24;
    array[27] = 4;
    array[28] = 90;
    array[29] = 1;*/
    final Comparable[] array = Util.randomArrayWithSeed(100_000, 100);
    BetterParallelMergeSort parallelMergeSort = new BetterParallelMergeSort(array);
    final ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() - 1);
    forkJoinPool.invoke(parallelMergeSort);
    Arrays.stream(array).forEach(item -> System.out.println(item));
  }
}
