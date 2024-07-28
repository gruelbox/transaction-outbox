package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.Utils;
import java.sql.ResultSet;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

@Builder
@RequiredArgsConstructor
public class DefaultSequenceGenerator implements SequenceGenerator, Validatable {
  private final Dialect dialect;

  @Override
  public long generate(Transaction tx, String topic) throws Exception {
    //noinspection resource
    var seqSelect = tx.prepareBatchStatement(dialect.getFetchNextSequence());
    seqSelect.setString(1, topic);
    try (ResultSet rs = seqSelect.executeQuery()) {
      long sequence = 1L;
      if (rs.next()) {
        sequence = rs.getLong(1) + 1;
        //noinspection resource
        var seqUpdate =
            tx.prepareBatchStatement("UPDATE TXNO_SEQUENCE SET seq = ? WHERE topic = ?");
        seqUpdate.setLong(1, sequence);
        seqUpdate.setString(2, topic);
        seqUpdate.executeUpdate();
      } else {
        try {
          //noinspection resource
          var seqInsert =
              tx.prepareBatchStatement("INSERT INTO TXNO_SEQUENCE (topic, seq) VALUES (?, ?)");
          seqInsert.setString(1, topic);
          seqInsert.setLong(2, sequence);
          seqInsert.executeUpdate();
        } catch (Exception e) {
          if (Utils.indexViolation(e)) {
            return generate(tx, topic);
          } else {
            throw e;
          }
        }
      }

      return sequence;
    }
  }

  @Override
  public void validate(Validator validator) {
    validator.notNull("dialect", dialect);
  }
}
