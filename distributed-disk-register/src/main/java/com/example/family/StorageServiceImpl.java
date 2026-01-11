package com.example.family;

import family.MessageId;
import family.StorageServiceGrpc;
import family.StoredMessage;
import family.StoreResult;
import io.grpc.stub.StreamObserver;

/**
 * 3. Aşama - StorageService implementasyonu.
 * Üyeler bu servis üzerinden mesaj saklama/okuma yapabilir.
 */
public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {

   private final MessageStore messageStore;

   public StorageServiceImpl(MessageStore messageStore) {
      this.messageStore = messageStore;
   }

   /**
    * Mesajı diske kaydeder.
    */
   @Override
   public void store(StoredMessage request, StreamObserver<StoreResult> responseObserver) {
      try {
         messageStore.set(request.getId(), request.getText());

         StoreResult result = StoreResult.newBuilder()
               .setSuccess(true)
               .build();

         responseObserver.onNext(result);
         responseObserver.onCompleted();

         System.out.println("📥 gRPC Store: id=" + request.getId());

      } catch (Exception e) {
         StoreResult result = StoreResult.newBuilder()
               .setSuccess(false)
               .setError(e.getMessage())
               .build();

         responseObserver.onNext(result);
         responseObserver.onCompleted();
      }
   }

   /**
    * Mesajı diskten okur.
    */
   @Override
   public void retrieve(MessageId request, StreamObserver<StoredMessage> responseObserver) {
      String text = messageStore.get(request.getId());

      StoredMessage message = StoredMessage.newBuilder()
            .setId(request.getId())
            .setText(text != null ? text : "")
            .build();

      responseObserver.onNext(message);
      responseObserver.onCompleted();

      System.out.println("📤 gRPC Retrieve: id=" + request.getId() +
            ", found=" + (text != null));
   }
}
