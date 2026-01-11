package com.example.family;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Basit in-memory mesaj deposu.
 * Şimdilik sadece bellekte tutuyor, 2. Aşamada disk'e kayıt eklenecek.
 */
public class MessageStore {

   private final ConcurrentHashMap<Integer, String> messages = new ConcurrentHashMap<>();

   /**
    * Mesajı kaydeder.
    */
   public void set(int id, String message) {
      messages.put(id, message);
      System.out.println("📦 Stored message: id=" + id + ", text=" + message);
   }

   /**
    * Mesajı getirir. Bulunamazsa null döner.
    */
   public String get(int id) {
      return messages.get(id);
   }

   /**
    * Toplam mesaj sayısını döner.
    */
   public int size() {
      return messages.size();
   }
}
