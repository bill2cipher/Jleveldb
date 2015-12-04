package com.lilith.leveldb.util;

/**
 * A very simple random number generator. Not especially good at
 * generating truly random bits. but good enough for our needs in 
 * this package.
 * @author Administrator
 *
 */
public class Random {
  private int seed = 0;
  private final static int M = 2147483647; // 2^31 - 1
  private final static int A = 16807;      // bits 14, 8, 7, 5, 2, 1, 0
  
  public Random(int seed) {
    this.seed = seed & 0X7FFFFFFF;
    if (this.seed == 0 || seed == 2147483647) seed = 1;
  }
  
  public int Next() {
    long product = seed * A;
    long result = (product >> 31) + (product & M);
    if (result > M) result -= M;
    seed = (int) (M & result);
    return seed;
  }
  
  public int Uniform(int n) { return Next() % n;}
  
  // randomly returns true ~ "1/n" of the time, and false otherwise.
  public boolean OneIn(int n) { return (Next() % n) == 0; }
  
  public int Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
  
}
