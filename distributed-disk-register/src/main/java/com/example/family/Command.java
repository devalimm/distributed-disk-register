package com.example.family;

/**
 * Basit komut sınıfı - SET ve GET komutlarını temsil eder.
 */
public class Command {

   public enum Type {
      SET,
      GET,
      UNKNOWN
   }

   private final Type type;
   private final int messageId;
   private final String messageText; // Sadece SET için kullanılır

   public Command(Type type, int messageId, String messageText) {
      this.type = type;
      this.messageId = messageId;
      this.messageText = messageText;
   }

   public Type getType() {
      return type;
   }

   public int getMessageId() {
      return messageId;
   }

   public String getMessageText() {
      return messageText;
   }

   /**
    * String komutunu parse eder.
    * Format: SET <id> <message> veya GET <id>
    */
   public static Command parse(String line) {
      if (line == null || line.trim().isEmpty()) {
         return new Command(Type.UNKNOWN, -1, null);
      }

      String[] parts = line.trim().split("\\s+", 3);

      if (parts.length < 2) {
         return new Command(Type.UNKNOWN, -1, null);
      }

      String cmd = parts[0].toUpperCase();

      try {
         int id = Integer.parseInt(parts[1]);

         if (cmd.equals("SET") && parts.length >= 3) {
            return new Command(Type.SET, id, parts[2]);
         } else if (cmd.equals("GET")) {
            return new Command(Type.GET, id, null);
         }
      } catch (NumberFormatException e) {
         // ID parse edilemedi
      }

      return new Command(Type.UNKNOWN, -1, null);
   }
}
