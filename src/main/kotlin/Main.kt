import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.decimal4j.immutable.Decimal4f
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.math.RoundingMode
import java.sql.SQLException
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.math.log

val logger: Logger = LoggerFactory.getLogger("Interest")

fun main(args: Array<String>) {
    val config = ConfigFactory.parseFile(File("backend.conf"))

    val account = config.getLong("account")

    val dataSource = getDataSource(config)

    dataSource.connection.use {
        it.createStatement()
            .execute("CREATE TABLE IF NOT EXISTS interest_payments (paid TIMESTAMP NOT NULL DEFAULT NOW())")
        it.createStatement()
            .execute("CREATE TABLE IF NOT EXISTS accumulated_interest (account BIGINT NOT NULL, amount NUMERIC(10, 4) NOT NULL, paid TIMESTAMP NOT NULL DEFAULT NOW())")
        it.autoCommit = false
        try {
            var start = System.nanoTime()

            it.createStatement().execute("INSERT INTO interest_payments VALUES (DATE_TRUNC('DAY', NOW()))")
            val paymentQuery = it.createStatement().executeQuery("SELECT * FROM interest_payments ORDER BY paid DESC LIMIT 2")
            val hasFirst = paymentQuery.next()
            if (!hasFirst) return

            val first = paymentQuery.getTimestamp("paid").toInstant().toEpochMilli()

            val hasSecond = paymentQuery.next()
            if (!hasSecond) return

            val diff = first - paymentQuery.getTimestamp("paid").toInstant().toEpochMilli()
            if (diff < TimeUnit.DAYS.toMillis(1)) {
                logger.error("Invalid interval, aborting")
                it.rollback()
                return
            } else if (diff > TimeUnit.DAYS.toMillis(1)) {
                logger.warn("MISSING INTEREST")
            }

            val resultSet = it.createStatement()
                .executeQuery("SELECT " +
                        "ledger.account AS account, " +
                        "SUM(CASE WHEN \"type\"='CREDIT' THEN ledger.amount ELSE -ledger.amount END) + COALESCE(SUM(accumulated_interest.amount), CAST(0 AS NUMERIC(10, 4))) AS amount " +
                        "FROM ledger " +
                        "LEFT JOIN accumulated_interest ON ledger.account = accumulated_interest.account " +
                        "INNER JOIN accounts ON ledger.account = accounts.id AND NOT accounts.closed AND accounts.code IS NOT NULL AND accounts.name = 'Holding Account' AND accounts.account_type = 'LIABILITY'" +
                        "GROUP BY ledger.account")

            val interestStmt = it.prepareStatement("INSERT INTO accumulated_interest (account, amount) VALUES (?, CAST(? AS NUMERIC(10, 4)))")
            while (resultSet.next()) {
                val interest = calculateInterest(resultSet.getString("amount"))
                if (interest != null) {
                    interestStmt.setLong(1, resultSet.getLong("account"))
                    interestStmt.setString(2, interest)
                    interestStmt.addBatch()
                }
            }
            interestStmt.executeBatch()

            logger.info("Paying interest (phase 1) took " + (System.nanoTime()-start)/1000000 + "ms")
            start = System.nanoTime()

            val interestDueStmt = it.createStatement()
                .executeQuery("SELECT account, SUM(amount) AS amount FROM accumulated_interest WHERE DATE_TRUNC('MONTH', paid) < DATE_TRUNC('MONTH', NOW()) GROUP BY account")
            val credit = it.prepareStatement("INSERT INTO ledger (account, referenced_account, \"type\", message, amount) VALUES (?, ?, 'CREDIT', ?, CAST(? AS NUMERIC(10, 4)))")
            val debit = it.prepareStatement("INSERT INTO ledger (account, referenced_account, \"type\", message, amount) VALUES (?, ?, 'DEBIT', ?, CAST(? AS NUMERIC(10, 4)))")
            while (interestDueStmt.next()) {
                val interestAccount = interestDueStmt.getLong("account")
                val interestAmount = interestDueStmt.getString("amount")

                credit.setLong(1, interestAccount)
                credit.setLong(2, account)
                credit.setString(3, "Monthly interest payment")
                credit.setString(4, interestAmount)
                credit.addBatch()

                debit.setLong(1, account)
                debit.setLong(2, interestAccount)
                debit.setString(3, "Monthly interest payment")
                debit.setString(4, interestAmount)
                debit.addBatch()
            }
            credit.executeBatch()
            debit.executeBatch()

            it.createStatement()
                .execute("DELETE FROM accumulated_interest WHERE DATE_TRUNC('MONTH', paid) < DATE_TRUNC('MONTH', NOW())")

            it.commit()
            logger.info("Paying interest (phase 2) took " + (System.nanoTime()-start)/1000000 + "ms")
        } catch (ex: Exception) {
            it.rollback()
            throw ex
        } finally {
            it.autoCommit = true
        }
    }
}

fun calculateInterest(amountStr: String): String? {
    var amount = Decimal4f.valueOf(amountStr, RoundingMode.UNNECESSARY)

    var total = Decimal4f.ZERO
    if (amount.isNegative) return null

    val part1 = amount.min(Decimal4f.valueOf(200))
        .multiplyBy(Decimal4f.valueOf("0.0149", RoundingMode.UNNECESSARY), RoundingMode.HALF_EVEN)
        .divide(30, RoundingMode.HALF_EVEN)
    amount = amount.subtract(Decimal4f.valueOf(200))
    total = total.add(part1)

    if (amount.isNegative) return total.toString()

    val part2 = amount.min(Decimal4f.valueOf(800))
        .multiplyBy(Decimal4f.valueOf("0.0025", RoundingMode.UNNECESSARY), RoundingMode.HALF_EVEN)
        .divide(30, RoundingMode.HALF_EVEN)
    amount = amount.subtract(Decimal4f.valueOf(800))
    total = total.add(part2)

    if (amount.isNegative) return total.toString()

    val part3 = amount
        .multiplyBy(Decimal4f.valueOf("0.0001", RoundingMode.UNNECESSARY), RoundingMode.HALF_EVEN)
        .divide(30, RoundingMode.HALF_EVEN)
    total = total.add(part3)

    return total.toString()
}

fun getDataSource(applicationConfig: Config): HikariDataSource {
    val config = HikariConfig()
    config.jdbcUrl = "jdbc:postgresql://localhost:${
        applicationConfig.getString("database.port")
    }/${applicationConfig.getString("database.database")}"
    config.username = applicationConfig.getString("database.username")
    config.password = applicationConfig.getString("database.password")
    return HikariDataSource(config)
}