package com.example.family;

import java.io.*;
import java.nio.file.*;

/**
 * Disk tabanlı mesaj deposu.
 * Her mesaj messages/ klasöründe ayrı bir dosyada saklanır.
 * Örn: SET 42 "test" → messages/42.msg
 */
public class MessageStore {

   private final Path messagesDir;

   public MessageStore() {
      // messages/ klasörünü oluştur
      this.messagesDir = Paths.get("messages");
      try {
         Files.createDirectories(messagesDir);
         System.out.println("📁 Messages directory: " + messagesDir.toAbsolutePath());
      } catch (IOException e) {
         System.err.println("Failed to create messages directory: " + e.getMessage());
      }
   }

   /**
    * Mesajı diske kaydeder.
    * Dosya adı: messages/<id>.msg
    */
   public void set(int id, String message) {
      Path file = messagesDir.resolve(id + ".msg");
      try (BufferedWriter writer = Files.newBufferedWriter(file)) {
         writer.write(message);
         System.out.println("Stored to disk: " + file.getFileName());
      } catch (IOException e) {
         System.err.println("Failed to write message " + id + ": " + e.getMessage());
      }
   }

   /**
    * Mesajı diskten okur. Bulunamazsa null döner.
    */
   public String get(int id) {
      Path file = messagesDir.resolve(id + ".msg");
      if (!Files.exists(file)) {
         return null;
      }
      try (BufferedReader reader = Files.newBufferedReader(file)) {
         StringBuilder sb = new StringBuilder();
         String line;
         while ((line = reader.readLine()) != null) {
            if (sb.length() > 0)
               sb.append("\n");
            sb.append(line);
         }
         return sb.toString();
      } catch (IOException e) {
         System.err.println("Failed to read message " + id + ": " + e.getMessage());
         return null;
      }
   }

   /**
    * Toplam mesaj sayısını döner.
    */
   public int size() {
      try {
         return (int) Files.list(messagesDir)
               .filter(p -> p.toString().endsWith(".msg"))
               .count();
      } catch (IOException e) {
         return 0;
      }
   }
}
