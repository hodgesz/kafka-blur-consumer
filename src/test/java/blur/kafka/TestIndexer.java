package blur.kafka;

import java.util.Random;

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

import blur.kafka.client.IIndexer;

public class TestIndexer implements IIndexer {

	private String _familiy = "testfamily";
	private String _table = "testtable";
	private RowMutation rowMutation = new RowMutation();
    private Random random = new Random();


	public RowMutation getMutation(byte[] payload) {

		rowMutation.clear();

		if (payload != null) {
			return indexMessage(new String(payload));

		} else
			return null;
	}

	private RowMutation indexMessage(String message) {

		try {
		    rowMutation.setRowId(Long.toString(random.nextLong()));
		    rowMutation.setTable(_table);
		    rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
		    Record record = new Record();
		    record.setFamily(_familiy);
		    record.setRecordId(Long.toString(random.nextLong()));
		    record.addToColumns(new Column("col", message));
		    rowMutation.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
		    return rowMutation;

		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}

	}
}
