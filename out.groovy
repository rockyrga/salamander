import groovy.sql.Sql
import groovy.json.*

@GrabConfig(systemClassLoader=true)
@Grab(group='mysql', module='mysql-connector-java', version='5.1.30')
class app {

  static DB_URL = "jdbc:mysql://localhost:3306/radium"
  static DB_USER = "dbuser"
  static DB_PASSWORD = "dbuser"
  static MYSQL = getMysqlConnection()

  static main(args) {

    def policyReferenceNumber = args[0]

    StringBuilder resultBuilder = new StringBuilder()

    resultBuilder.append(getUnderwriterJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getAgentJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getPolicyJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getRiskAddressJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getPropertyDetailJsonString(policyReferenceNumber))

    if(hasBuilding(policyReferenceNumber)) {
      resultBuilder.append(",\n\n").append(getBuildingsJsonString(policyReferenceNumber))
    }

    if(hasContent(policyReferenceNumber)) {
      resultBuilder.append(",\n\n").append(getContentsJsonString(policyReferenceNumber))
    }

    if(hasPersonalPossession(policyReferenceNumber)) {
      resultBuilder.append(",\n\n").append(getPersonalPossessionJsonString(policyReferenceNumber))
    }

    if(hasLegalExpense(policyReferenceNumber)) {
      resultBuilder.append(",\n\n").append(getLegalExpenseJsonString(policyReferenceNumber))
    }

    resultBuilder.append(",\n\n").append(getRiskQuestionsJsonString(policyReferenceNumber))
    // getPolcyholdersJsonString(policyReferenceNumber)
    resultBuilder.append(",\n\n").append(getClausesJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getExcessesJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getPaymentJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getLoadingsJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getReferralsJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getStepsJsonString(policyReferenceNumber))
    resultBuilder.append(",\n\n").append(getNoteJsonString(policyReferenceNumber))

    println resultBuilder.toString()
  }

  static getUnderwriterJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from underwriters where id = (select underwriter_id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "underwriters")
  }

  static getAgentJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from agents where id = (select agent_id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "agents")
  }

  static getPolicyJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from policies where reference_number = $referenceNumber")
    //println "column size=" + columns.size() + " columns = " + columns + " values = " + values

    def policyMap = toMap(columns, values)
    //policyMap.each{ key, value -> println "${key}:${value}" }

    return toJson(policyMap, "policies")
  }

  static getRiskAddressJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from risk_addresses where policy_id = (select id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "risk_addresses")
  }

  static getPropertyDetailJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from property_details where policy_id = (select id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "property_details")
  }

  static hasBuilding(referenceNumber) {

    def countRows = MYSQL.firstRow("select count(id) as numberOfRows from buildings where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return countRows.numberOfRows > 0
  }

  static getBuildingsJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from buildings where policy_id = (select id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "buildings")
  }

  static hasContent(referenceNumber) {

    def countRows = MYSQL.firstRow("select count(id) as numberOfRows from contents where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return countRows.numberOfRows > 0
  }

  static getContentsJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from contents where policy_id = (select id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "contents")
  }

  static hasPersonalPossession(referenceNumber) {

    def countRows = MYSQL.firstRow("select count(id) as numberOfRows from personal_possessions where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return countRows.numberOfRows > 0
  }

  static getPersonalPossessionJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from personal_possessions where policy_id = (select id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "personal_possessions")
  }

  static hasLegalExpense(referenceNumber) {

    def countRows = MYSQL.firstRow("select count(id) as numberOfRows from legal_expenses where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return countRows.numberOfRows > 0
  }

  static getLegalExpenseJsonString(referenceNumber) {

    def columns = [], values = []
    (columns, values) = toColumnAndValue("select * from legal_expenses where policy_id = (select id from policies where reference_number = $referenceNumber)")
    def policyMap = toMap(columns, values)

    return toJson(policyMap, "legal_expensess")
  }

  static getRiskQuestionsJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_risk_questions where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_risk_questions")
  }

  static getClausesJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_clauses where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_clauses")
  }

  static getExcessesJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_excesses where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_excesses")
  }

  static getPaymentJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from payments where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "payments")
  }

  static getLoadingsJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_loadings where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_loadings")
  }

  static getReferralsJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_referrals where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_referralss")
  }

  static getStepsJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_steps where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_steps")
  }

  static getNoteJsonString(referenceNumber) {

    def rows = MYSQL.rows("select * from policy_notes where policy_id = (select id from policies where reference_number = $referenceNumber)")

    return toJson(rows, "policy_notes")
  }

  static toColumnAndValue(sql) {

    def columns = [], values = []

    MYSQL.eachRow(sql,
    { meta->

        for (index in 0..< meta.columnCount) {
          columns << "${meta.getColumnLabel(index + 1)}"
        }

    },
    { row->

        for(int index = 0; index < columns.size(); index++) {
          values << row[index]
        }
    })

    return [columns, values]
  }

  static toMap(columns, values) {

    def map = [:]
    for(int index = 0; index < columns.size(); index++) {
        map.put(columns[index], values[index])
    }

    return map
  }

  static toJson(map, tableName) {

    def jsonContent = new JsonBuilder(map).toPrettyString()
    def isPrintSquareBrackets = !(map instanceof List)

    return "\"" + tableName + "\": ${isPrintSquareBrackets ? '[' : ''}${jsonContent}${isPrintSquareBrackets ? ']' : ''}"
  }

  static getMysqlConnection() {

    def dbUrl      = DB_URL
    def dbUser     = DB_USER
    def dbPassword = DB_PASSWORD
    def dbDriver   = "com.mysql.jdbc.Driver"

    return Sql.newInstance(dbUrl, dbUser, dbPassword, dbDriver)
  }

}
