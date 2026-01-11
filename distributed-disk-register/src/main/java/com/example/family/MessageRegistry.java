package com.example.family;

import family.NodeInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mesaj ID → Hangi üyelerde saklandığı bilgisini tutan registry.
 * Lider bu bilgiyi kullanarak GET isteklerinde doğru üyeye yönlendirir.
 */
public class MessageRegistry {

   // mesaj_id → [üye listesi]
   private final Map<Integer, List<NodeInfo>> messageLocations = new ConcurrentHashMap<>();

   /**
    * Mesajın hangi üyelerde saklandığını kaydeder.
    */
   public void registerMessage(int messageId, List<NodeInfo> members) {
      messageLocations.put(messageId, new ArrayList<>(members));
   }

   /**
    * Mesajın tutulduğu üyeleri döner. Bulunamazsa boş liste döner.
    */
   public List<NodeInfo> getMembers(int messageId) {
      return messageLocations.getOrDefault(messageId, Collections.emptyList());
   }

   /**
    * Toplam kayıtlı mesaj sayısı.
    */
   public int size() {
      return messageLocations.size();
   }

   /**
    * Tüm mesaj lokasyonlarını yazdırır (debug için).
    */
   public void printStatus() {
      System.out.println("📊 Message Registry Status:");
      System.out.println("   Total messages tracked: " + messageLocations.size());

      // Her üyenin kaç mesaj tuttuğunu say
      Map<String, Integer> memberCounts = new HashMap<>();
      for (List<NodeInfo> members : messageLocations.values()) {
         for (NodeInfo member : members) {
            String key = member.getHost() + ":" + member.getPort();
            memberCounts.merge(key, 1, Integer::sum);
         }
      }

      for (Map.Entry<String, Integer> entry : memberCounts.entrySet()) {
         System.out.println("   " + entry.getKey() + " → " + entry.getValue() + " messages");
      }
   }
}
