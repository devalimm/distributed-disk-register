package com.example.family;

import java.io.*;
import java.nio.file.*;

/**
 * tolerance.conf dosyasını okuyan sınıf.
 * Format: TOLERANCE=2
 */
public class ToleranceConfig {

   private static final String CONFIG_FILE = "tolerance.conf";
   private static final int DEFAULT_TOLERANCE = 1;

   private final int tolerance;

   public ToleranceConfig() {
      this.tolerance = readTolerance();
      System.out.println("⚙️ Tolerance config loaded: " + tolerance);
   }

   private int readTolerance() {
      Path configPath = Paths.get(CONFIG_FILE);

      if (!Files.exists(configPath)) {
         System.out.println("⚠️ tolerance.conf not found, using default: " + DEFAULT_TOLERANCE);
         return DEFAULT_TOLERANCE;
      }

      try (BufferedReader reader = Files.newBufferedReader(configPath)) {
         String line;
         while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("TOLERANCE=")) {
               String value = line.substring("TOLERANCE=".length()).trim();
               int t = Integer.parseInt(value);
               if (t < 1)
                  t = 1;
               return t;
            }
         }
      } catch (IOException | NumberFormatException e) {
         System.err.println("Error reading tolerance.conf: " + e.getMessage());
      }

      return DEFAULT_TOLERANCE;
   }

   public int getTolerance() {
      return tolerance;
   }
}
