package meetup.beeno.utils

import groovy.sql.Sql

// Usage
// Add your thin driver to the lib directory
// Check all def statements and adjust them to your own linking
// Run it
// Modify the generated output entity to your liking (i.e. add class and @HEntity header)
// The index statements need to be modified to indicate the timestamp sort order
class EntityGenerator {
	
	static void main(String[] args) {
		
		// convert the underscore DB column names to camel case
		// change the mapName method to your own preferred mapping method
		// if you don't want any conversion you can use "def mapName = {it}"
		def capitalize = {it.size()==1?it.toUpperCase():it[0].toUpperCase() + it[1 .. it.size() - 1]}		
		def mapName = { it.split("_").collect {	word -> capitalize(word.toLowerCase())	}.join("") }
		
		def mapping = ["NUMBER":"Long", "VARCHAR2":"String", "DATE":"Long"]
		
		def driver = "oracle.jdbc.driver.OracleDriver"
		def conn = "jdbc:oracle:thin:@---MYDBIP---:1521:---MYDBSID---"
		def query = "select * from profile_comment where rownum<2"
		def user = "DBUSER";
		def pw = "DBPASSWORD"
		
		def rowKey = mapName("MEMBER_ID")
		def indexed = ["PROFILE_MEMBER_ID"].collect{mapName(it)}
		def family = "props"
		def timestampCol = "updated"
				
		def sql = Sql.newInstance(conn, user, pw, driver)

		//Map<String, String> cols = new HashMap<String, String>()
		def cols =[:]
		sql.query(query){ rs ->
			rs.next()
			def meta = rs.metaData
			if(meta.columnCount <=0) return
				for(i in 1..meta.columnCount){
					cols.put mapName(meta.getColumnLabel(i)), mapping.get(meta.getColumnTypeName(i))
				}
		}		
		
		// spit out members
		cols.each{col -> println "${col.value} ${col.key};" }
		
		// spit out getters/setters
		cols.each{col ->
			if(col.key==rowKey){
			    println "\n@HRowKey"
			} else {
			    print """\n@HProperty(family="${family}", name="${col.key}\""""
			    if(indexed.contains(col.key)){
				print """, indexes = {@HIndex(date_col="${family}:${timestampCol}", date_invert=XXXCHANGEMEXXX)}"""
			    }
			    println ")"
			}
			print """public ${col.value} get${col.key}() { return this.${col.key}; }\npublic void set${col.key}(${col.value} ${col.key}) { this.${col.key} = ${col.key}; }\n"""
		}
	}
}
