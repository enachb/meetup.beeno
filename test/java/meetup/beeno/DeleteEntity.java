package meetup.beeno;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import meetup.beeno.util.HUtil;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteEntity {

	private static HUtil _hUtil;
	private static EntityService<SimpleEntity> _service;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		HBaseConfiguration conf = new HBaseConfiguration();
		conf.set("hbase.master", "localhost");
		HTablePool pool = new HTablePool(conf, 10);

		_hUtil = new HUtil();
		_hUtil.setPool(pool);

		HBaseAdmin admin = new HBaseAdmin(conf);

		// drop tables
		try {
			admin.disableTable("test_simple-by_photoId");
			admin.deleteTable("test_simple-by_photoId");
		} catch (TableNotFoundException e) {
			// silently swallow
		}
		try {
			admin.disableTable("test_simple");
			admin.deleteTable("test_simple");
		} catch (TableNotFoundException e) {
			// silently swallow
		}
		try {
			admin.disableTable("test_simple-by_intcol");
			admin.deleteTable("test_simple-by_intcol");
		} catch (TableNotFoundException e) {
			// silently swallow
		}

		// create tables & index
		HTableDescriptor test_simple = new HTableDescriptor("test_simple");
		test_simple.addFamily(new HColumnDescriptor("props"));
		admin.createTable(test_simple);

		HTableDescriptor by_photoId_idx = new HTableDescriptor("test_simple-by_photoId");
		by_photoId_idx.addFamily(new HColumnDescriptor("__idx__"));
		by_photoId_idx.addFamily(new HColumnDescriptor("props"));
		admin.createTable(by_photoId_idx);

		HTableDescriptor by_intcol = new HTableDescriptor("test_simple-by_intcol");
		by_intcol.addFamily(new HColumnDescriptor("__idx__"));
		by_intcol.addFamily(new HColumnDescriptor("props"));
		admin.createTable(by_intcol);

		_service = EntityService.create(SimpleEntity.class);

		for (int i = 0; i < 10; i++) {
			SimpleEntity se = new SimpleEntity();
			se.setId("r:" + i);
			se.setStringProperty("superman");
			se.setIntProperty(10);
			se.setPhotoIdProperty("PHOTOID" + i);
			_service.save(se);
		}

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testDeleteByEntity() throws HBaseException {

		// check if we have the expected entity in there
		SimpleEntity entity = _service.get("r:0");
		assertEquals("PHOTOID0", entity.getPhotoIdProperty());

		// delete it
		assertTrue(_service.delete(entity));
		SimpleEntity nonExistingEntity = _service.get("r:0");
		assertNull(nonExistingEntity);

	}

	@Test
	public void testIndexEntryRemoval() throws HBaseException {

		// find entity by index
		Query query = _service.query().using(Criteria.eq("photoIdProperty", "PHOTOID1"));
		List<SimpleEntity> items = query.execute();
		assertEquals(1, items.size());

		// find entities by other index
		query = _service.query().using(Criteria.eq("photoIdProperty", "PHOTOID1"));
		items = query.execute();
		assertEquals(1, items.size());

		// delete it
		assertTrue(_service.delete(items.get(0)));

		// check if we deleted the index entry
		query = _service.query().using(Criteria.eq("photoIdProperty", "PHOTOID1"));
		items = query.execute();
		assertEquals(0, items.size());

	}

	@Test
	public void testIndexEntryRemovalOtherIndex() throws HBaseException {

		// find entity by index
		Query query = _service.query().using(Criteria.eq("photoIdProperty", "PHOTOID2"));
		List<SimpleEntity> items = query.execute();
		assertEquals(1, items.size());

		query = _service.query().using(Criteria.eq("intProperty", 10));
		items = query.execute();
		int preDeleteSize = items.size();

		// delete it
		assertTrue(_service.delete(items.get(0)));

		// check if we deleted the index entry
		query = _service.query().using(Criteria.eq("intProperty", 10));
		items = query.execute();
		assertEquals(preDeleteSize - 1, items.size());

	}

}
