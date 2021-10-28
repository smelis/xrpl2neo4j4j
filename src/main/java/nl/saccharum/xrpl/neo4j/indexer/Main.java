package nl.saccharum.xrpl.neo4j.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.xrpl.xrpl4j.model.client.ledger.LedgerResult;
import org.xrpl.xrpl4j.model.client.transactions.TransactionResult;
import org.xrpl.xrpl4j.model.jackson.ObjectMapperFactory;
import org.xrpl.xrpl4j.model.transactions.TransactionMetadata;
import org.xrpl.xrpl4j.model.transactions.TransactionType;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okhttp3.HttpUrl;
import org.neo4j.driver.Query;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.xrpl.xrpl4j.client.JsonRpcClientErrorException;
import org.xrpl.xrpl4j.client.XrplClient;
import org.xrpl.xrpl4j.model.client.common.LedgerSpecifier;
import org.xrpl.xrpl4j.model.client.ledger.LedgerRequestParams;
import org.xrpl.xrpl4j.model.ledger.LedgerHeader;
import org.xrpl.xrpl4j.model.transactions.AffectedNode;
import org.xrpl.xrpl4j.model.transactions.CreatedNode;
import org.xrpl.xrpl4j.model.transactions.ImmutablePayment;
import org.xrpl.xrpl4j.model.transactions.Memo;
import org.xrpl.xrpl4j.model.transactions.NewFields;

/**
 * Rudimentary indexer of XRP ledgers in Neo4J using xrpl4j. 4j.
 * Based on (javascript) code by Thomas Silkjaer published here:
 * https://github.com/Dassy23/xrpl-to-neo4j
 * NB Transaction deserialization currently builds on an as yet unmerged PR for xrpl4j.
  */
public class Main implements AutoCloseable {

    private final List<String> genesisWallets = Arrays.asList("r8TR1AeB1RDQFabM6i8UoFsRF5basqoHJ", "rB5TihdPbKgMrkFqrqUC3yLdE8hhv4BdeY", "rJYMACXJd1eejwzZA53VncYmiK2kZSBxyD", "rsjB6kHDBDUw7iB5A1EVDK1WmgmR6yFKpB", "rGRGYWLmSvPuhKm4rQV287PpJUgTB1VeD7", "rUzSNPtxrmeSTpnjsvaTuQvF2SQFPFSvLn", "rNRG8YAUqgsqoE5HSNPHTYqEGoKzMd7DJr", "r43mpEMKY1cVUX8k6zKXnRhZMEyPU9aHzR", "r9ssnjg97d86PxMrjVsCAX1xE9qg8czZTu", "rppWupV826yJUFd2zcpRGSjQHnAHXqe7Ny", "rB59DESmVnTwXd2SCy1G4ReVkP5UM7ZYcN", "rDCJ39V8yW39Ar3Pod7umxnrp24jATE1rt", "rf7phSp1ABzXhBvEwgSA7nRzWv2F7K5VM7", "rHDcKZgR7JDGQEe9r13UZkryEVPytV6L6F", "rUf6pynZ8ucVj1jC9bKExQ7mb9sQFooTPK", "rhWcbzUj9SVJocfHGLn58VYzXvoVnsU44u", "rnj8sNUBCw3J6sSstY9QDDoncnijFwH7Cs", "rLqQ62u51KR3TFcewbEbJTQbCuTqsg82EY", "rGow3MKvbQJvuzPPP4vEoohGmLLZ5jXtcC", "rUvEG9ahtFRcdZHi3nnJeFcJWhwXQoEkbi", "rBQQwVbHrkf8TEcW4h4MtE6EUyPQedmtof", "rKMhQik9qdyq8TDCYT92xPPRnFtuq8wvQK", "rLeRkwDgbPVeSakJ2uXC2eqR8NLWMvU3kN", "rsRpe4UHx6HB32kJJ3FjB6Q1wUdY2wi3xi", "rpWrw1a5rQjZba1VySn2jichsPuB4GVnoC", "rpGaCyHRYbgKhErgFih3RdjJqXDsYBouz3", "rKZig5RFv5yWAqMi9PtC5akkGpNtn3pz8A", "r3AthBf5eW4b9ujLoXNHFeeEJsK3PtJDea", "rNWzcdSkXL28MeKaPwrvR3i7yU6XoqCiZc", "ramPgJkA1LSLevMg2Yrs1jWbqPTsSbbYHQ", "rHrSTVSjMsZKeZMenkpeLgHGvY5svPkRvR", "rPFPa8AjKofbPiYNtYqSWxYA4A9Eqrf9jG", "r3WjZU5LKLmjh8ff1q2RiaPLcUJeSU414x", "rBY8EZDiCNMjjhrC7SCfaGr2PzGWtSntNy", "r43ksW5oFnW7FMjQXDqpYGJfUwmLan9dGo", "rwoE5PxARitChLgu6VrMxWBHN7j11Jt18x", "rMNKtUq5Z5TB5C4MJnwzUZ3YP7qmMGog3y", "rGqM8S5GnGwiEdZ6QRm1GThiTAa89tS86E", "rLBwqTG5ErivwPXGaAGLQzJ2rr7ZTpjMx7", "rhuCtPvq6jJeYF1S7aEmAcE5iM8LstSrrP", "r4HabKLiKYtCbwnGG3Ev4HqncmXWsCtF9F", "rDa8TxBdCfokqZyyYEpGMsiKziraLtyPe8", "rPrz9m9yaXT94nWbqEG2SSe9kdU4Jo1CxA", "rJ6VE6L87yaVmdyxa9jZFXSAdEFSoTGPbE", "r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV", "rHTxKLzRbniScyQFGMb3NodmxA848W8dKM", "rnp8kFTTm6KW8wsbgczfmv56kWXghPSWbK", "rf8kg7r5Fc8cCszGdD2jeUZt2FrgQd76BS", "rBJwwXADHqbwsp6yhrqoyt2nmFx9FB83Th", "rMNzmamctjEDqgwyBKbYfEzHbMeSkLQfaS", "rHSTEtAcRZBg1SjcR4KKNQzJKF3y86MNxT", "rEe6VvCzzKU1ib9waLknXvEXywVjjUWFDN", "rJZCJ2jcohxtTzssBPeTGHLstMNEj5D96n", "rQsiKrEtzTFZkQjF9MrxzsXHCANZJSd1je", "rHXS898sKZX6RY3WYPo5hW6UGnpBCnDzfr", "rPcHbQ26o4Xrwb2bu5gLc3gWUsS52yx1pG", "r3PDtZSa5LiYp1Ysn1vMuMzB59RzV3W9QH", "rhdAw3LiEfWWmSrbnZG3udsN7PoWKT56Qo", "rLs1MzkFWCxTbuAHgjeTZK4fcCDDnf2KRv", "rUnFEsHjxqTswbivzL2DNHBb34rhAgZZZK", "r4mscDrVMQz2on2px31aV5e5ouHeRPn8oy", "rLCvFaWk9WkJCCyg9Byvwbe9gxn1pnMLWL", "rLzpfV5BFjUmBs8Et75Wurddg4CCXFLDFU", "rUy6q3TxE4iuVWMpzycrQfD5uZok51g1cq", "rMwNkcpvcJucoWbFW89EGT6TfZyGUkaGso", "rPhMwMcn8ewJiM6NnP6xrm9NZBbKZ57kw1", "rnT9PFSfAnWyj2fd7D5TCoCyCYbK4n356A", "rEyhgkRqGdCK7nXtfmADrqWYGT6rSsYYEZ", "rJFGHvCtpPrftTmeNAs8bYy5xUeTaxCD5t", "rNSnpURu2o7mD9JPjaLsdUw2HEMx5xHzd", "rUZRZ2b4NyCxjHSQKiYnpBuCWkKwDWTjxw", "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59", "rauPN85FeNYLBpHgJJFH6g9fYUWBmJKKhs", "rEMqTpu21XNk62QjTgVXKDig5HUpNnHvij", "rDngjhgeQZj9FNtW8adgHvdpMJtSBMymPe", "rEJkrunCP8hpvk4ijxUgEWnxCE6iUiXxc2", "rLCAUzFMzKzcyRLa1B4LRqEMsUkYXX1LAs", "r4cmKj1gK9EcNggeHMy1eqWakPBicwp69R", "rnNPCm97TBMPprUGbfwqp1VpkfHUqMeUm7", "rwZpVacRQHYArgN3NzUfuKEcRDfbdvqGMi", "rfCXAzsmsnqDvyQj2TxDszTsbVj5cRTXGM", "rfpQtAXgPpHNzfnAYykgT6aWa94xvTEYce", "r4U5AcSVABL6Ym85jB94KYnURnzkRDqh1Y", "rHzWtXTBrArrGoLDixQAgcSD2dBisM19fF", "r9hEDb4xBGRfBCcX3E4FirDWQBAYtpxC8K", "r2oU84CFuT4MgmrDejBaoyHNvovpMSPiA", "rVehB9r1dWghqrzJxY2y8qTiKxMgHFtQh", "rsQP8f9fLtd58hwjEArJz2evtrKULnCNif", "rMkq9vs7zfJyQSPPkS2JgD8hXpDR5djrTA", "r4q1ujKY4hwBpgFNFx43629f2LuViU4LfA", "rhDfLV1hUCanViHnjJaq3gF1R2mo6PDCSC", "rwDWD2WoU7npQKKeYd6tyiLkmr7DuyRgsz", "rBrspBLnwBRXEeszToxcDUHs4GbWtGrhdE", "rLebJGqYffmcTbFwBzWJRiv5fo2ccmmvsB", "rPWyiv5PXyKWitakbaKne4cnCQppRvDc5B", "rHWKKygGWPon9WSj4SzTH7vS4ict1QWKo9", "rGwUWgN5BEg3QGNY3RX2HfYowjUTZdid3E", "rKHD6m92oprEVdi1FwGfTzxbgKt8eQfUYL", "r9duXXmUuhSs6JxKpPCSh2tPUg9AGvE2cG", "rphasxS8Q5p5TLTpScQCBhh5HfJfPbM2M8", "rU5KBPzSyPycRVW1HdgCKjYpU6W9PKQdE8", "r4DGz8SxHXLaqsA9M2oocXsrty6BMSQvw3", "rBnmYPdB5ModK8NyDUad1mxuQjHVp6tAbk", "rfitr7nL7MX85LLKJce7E3ATQjSiyUPDfj", "rD1jovjQeEpvaDwn9wKaYokkXXrqo4D23x", "rJQx7JpaHUBgk7C56T2MeEAu1JZcxDekgH", "r9aRw8p1jHtR9XhDAE22TjtM7PdupNXhkx", "rM1oqKtfh1zgjdAgbFmaRm3btfGBX25xVo", "rPgrEG6nMMwAM1VbTumL23dnEX4UmeUHk7", "rLp9pST1aAndXTeUYFkpLtkmtZVNcMs2Hc", "rnziParaNb8nsU4aruQdwYE3j5jUcqjzFm", "rwpRq4gQrb58N7PRJwYEQaoSui6Xd3FC7j", "rMYBVwiY95QyUnCeuBQA1D47kXA9zuoBui", "rGLUu9LfpKyZyeTtSRXpU15e2FfrdvtADa", "rhxbkK9jGqPVLZSWPvCEmmf15xHBfJfCEy", "rHC5QwZvGxyhC75StiJwZCrfnHhtSWrr8Y", "r49pCti5xm7WVNceBaiz7vozvE9zUGq8z2", "rKdH2TKVGjoJkrE8zQKosL2PCvG2LcPzs5", "rBqCdAqw7jLH3EDx1Gkw4gUAbFqF7Gap4c", "rwCYkXihZPm7dWuPCXoS3WXap7vbnZ8uzB", "rnCiWCUZXAHPpEjLY1gCjtbuc9jM1jq8FD", "rp1xKo4CWEzTuT2CmfHnYntKeZSf21KqKq", "rDJvoVn8PyhwvHAWuTdtqkH4fuMLoWsZKG", "rEA2XzkTXi6sWRzTVQVyUoSX4yJAzNxucd", "rshceBo6ftSVYo8h5uNPzRWbdqk4W6g9va", "rBKPS4oLSaV2KVVuHH8EpQqMGgGefGFQs7", "rEUXZtdhEtCDPxJ3MAgLNMQpq4ASgjrV6i", "rnGTwRTacmqZZBwPB6rh3H1W4GoTZCQtNA", "rJRyob8LPaA3twGEQDPU2gXevWhpSgD8S6", "rJ51FBSh6hXSUkFdMxwmtcorjx9izrC1yj", "rnxyvrF2mUhK6HubgPxUfWExERAwZXMhVL", "rDsDR1pFaY8Ythr8px4N98bSueixyrKvPx", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", "rDy7Um1PmjPgkyhJzUWo1G8pzcDan9drox", "rLiCWKQNUs8CQ81m2rBoFjshuVJviSRoaJ", "rEWDpTUVU9fZZtzrywAUE6D6UcFzu6hFdE");
    private final Driver neo4jDriver;
    private final String database;
    private static final long START_LEDGER = 32570;
    // first ledger with a transaction
    // private static final long START_LEDGER = 38128;
    private final ObjectMapper objectMapper = ObjectMapperFactory.create();
    private final Cacher cache;
    private final HttpUrl rippledUrl = HttpUrl.get("https://s2.ripple.com:51234/");
    private final XrplClient xrplClient;

    public Main(String uri, String user, String password, String database, Path cachePath) {
        this.neo4jDriver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.cache = new Cacher(cachePath);
        this.database = database;
        System.out.println(rippledUrl.toString());
        xrplClient = new XrplClient(rippledUrl);
    }

    @Override
    public void close() {
        neo4jDriver.close();
    }

    public void run() throws URISyntaxException, InterruptedException, IOException, JsonRpcClientErrorException {
        long ledgerIndex = getLastIndexedLedger();
        if (ledgerIndex == -1) {
            initialSetup();
            ledgerIndex = START_LEDGER;
        }
        // Limit to the next 100k ledgers
        for (long i = ledgerIndex; i < ledgerIndex + 1_00_000; i++) {
            LedgerResult ledger = getLedger(i);
            createLedgerNode(ledger.ledger());
            List<TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction>> payments = paymentsFromTransactions(ledger.ledger().transactions());
            activationsFromPayments(payments).forEach((tr) -> {
                createChildWalletAndActivation(ledger, tr);
            });
            payments.forEach((payment) -> {
                createPaymentRelation(ledger, payment);
            });

            // Play nice
            Thread.sleep(25);
            if (i % 20 == 0) {
                Thread.sleep(1000);
            }
        }
    }

    private long getLastIndexedLedger() {
        try (Session session = neo4jDriver.session(SessionConfig.forDatabase(database))) {
            Result result = session.run("MATCH (l:Ledger) RETURN l.ledgerIndex ORDER BY l.ledgerIndex DESC LIMIT 1");
            return result.single().get("l.ledgerIndex", -1);
        } catch (NoSuchRecordException ex) {
            return -1;
        }
    }

    private boolean initialSetup() {
        boolean result = true;
        try (Session session = neo4jDriver.session(SessionConfig.forDatabase(database))) {
            final Transaction tx1 = session.beginTransaction();
            try {
                tx1.run("CREATE CONSTRAINT ON (o:Ledger) ASSERT o.ledgerHash IS UNIQUE");
                tx1.run("CREATE CONSTRAINT ON (o:Wallet) ASSERT o.address IS UNIQUE");
                tx1.run("CREATE CONSTRAINT ON (o:Payment) ASSERT o.hash IS UNIQUE");
                tx1.run("CREATE INDEX ON :Ledger(ledgerIndex)");
                tx1.run("CREATE INDEX ON :Ledger(closeTime)");
                tx1.run("CREATE INDEX ON :Payment(ledgerIndex)");
                tx1.run("CREATE INDEX ON :Payment(date)");
                tx1.run("CREATE INDEX ON :Payment(amount)");
                tx1.commit();
            } catch (org.neo4j.driver.exceptions.ClientException ex) {
                tx1.close();
                System.err.println(ex.getMessage()); // TODO: already exist (how do I check?)
                result = false;
            }

            createWalletNode("genesis");
            Map<String, Object> ledgerParams = new HashMap<>();
            ledgerParams.put("ledgerIndex", 1l);
            ledgerParams.put("ledgerHash", 1l);
            ledgerParams.put("closeTime", null);
            ledgerParams.put("parentHash", 0l);
            ledgerParams.put("totalCoins", 0l);
            createLedgerNode(ledgerParams);

            Map<String, Object> params = new HashMap<>();
            params.put("parent", "genesis");
            params.put("ledgerIndex", 1l);
            params.put("hash", 1l);
            params.put("date", null);
            params.put("amount", 0l);
            genesisWallets.forEach((t) -> {
                createWalletNode(t);
                params.put("child", t);
                createActivation(params);
            });
        }
        return result;
    }

    private LedgerResult getLedger(long ledgerIndex) throws IOException, InterruptedException, URISyntaxException, JsonRpcClientErrorException {
        System.out.println("Getting ledger: " + ledgerIndex);
        String cached = cache.getCachedLedger(ledgerIndex);
        if (null != cached) {
            return objectMapper.readValue(cached, LedgerResult.class);

        } else {
            LedgerResult result = null;
            try {
                result = xrplClient.ledger(LedgerRequestParams.builder().ledgerSpecifier(LedgerSpecifier.of((int) ledgerIndex)).transactions(true).build());
            } catch (JsonRpcClientErrorException ex) {
                System.err.println(ex.getMessage());
                System.out.println("Waiting 60s then trying again...");
                Thread.sleep(60000);
                result = xrplClient.ledger(LedgerRequestParams.builder().ledgerSpecifier(LedgerSpecifier.of((int) ledgerIndex)).transactions(true).build());
            }
            if (result != null) {
                cache.cacheLedger(objectMapper.writeValueAsString(result), result.ledger().ledgerIndex().unsignedIntegerValue().bigIntegerValue().longValueExact());
            }
            return result;
        }
    }

    private void createPaymentRelation(LedgerResult ledgerResult, TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction> tr) {
        ImmutablePayment ip = ((ImmutablePayment) tr.transaction());

        Map<String, Object> params = new HashMap<>();
        params.put("sender", ip.account().value());
        params.put("receiver", ip.destination().value());
        params.put("ledgerIndex", ledgerResult.ledgerIndexSafe().unsignedLongValue().longValue());
        params.put("date", ledgerResult.ledger().closeTimeHuman().get());
        params.put("hash", tr.hash().value());
        params.put("fee", ip.fee().value().longValue() / 1000000);
        params.put("isActivation", isActivationPayment(tr));
        ip.memos().forEach((t) -> {
            Memo memo = t.memo();
            if (memo != null) {
                // TODO: Figure out memo data
                System.out.println("Memo: " + memo.memoData().orElse("< no memo data>"));
            }
        });
        if (ip.sourceTag().isPresent()) {
            params.put("sourceTag", ip.sourceTag().get().longValue());
        }
        if (ip.destinationTag().isPresent()) {
            params.put("destinationTag", ip.destinationTag().get().longValue());
        }
        ip.amount().handle(
                (xrp) -> {
                    params.put("amount", xrp.value().longValue());
                    params.put("amountCurrency", "xrp");
                },
                (issued) -> {
                    params.put("amount", Float.parseFloat(issued.value()));
                    params.put("amountCurrency", issued.currency());
                    params.put("amountCurrencyIssuer", issued.issuer().value());
                });
        if (tr.metadata().get().deliveredAmount().isPresent()) {
            tr.metadata().get().deliveredAmount().get().handle(
                    (xrp) -> {
                        params.put("deliveredAmount", xrp.toXrp().floatValue());
                        params.put("deliveredCurrency", "xrp");
                    },
                    (issued) -> {
                        params.put("deliveredAmount", Float.parseFloat(issued.value()));
                        params.put("deliveredCurrency", issued.currency());
                        params.put("deliveredCurrencyIssuer", issued.issuer().value());
                    }
            );
        }
        // TODO: MERGE iso MATH on wallets + add ON CREATE / ON MATCH SET to allow
        // for out-of-order creation (and, thus, parallization)
        runQuery("MATCH (sender:Wallet { address: $sender })\n "
                + "MATCH (receiver:Wallet { address: $receiver })\n "
                + "MERGE (sender)-[:PAYS]->(payment:Payment {hash: $hash})-[:RECEIVES]->(receiver)\n "
                + " ON CREATE SET "
                + " payment.hash = $hash, "
                + " payment.date = datetime($date), "
                + " payment.ledgerIndex = $ledgerIndex, "
                + " payment.fee = $fee, "
                + " payment.isActivation = $isActivation, "
                + (params.containsKey("sourceTag") ? " payment.sourceTag = $sourceTag, " : "")
                + (params.containsKey("destinationTag") ? " payment.destinationTag = $destinationTag, " : "")
                + (params.containsKey("deliveredCurrencyIssuer") ? " payment.deliveredCurrencyIssuer = $deliveredCurrencyIssuer, " : "")
                + (params.containsKey("deliveredAmount") ? " payment.deliveredAmount = $deliveredAmount, " : "")
                + (params.containsKey("deliveredCurrency") ? " payment.deliveredCurrency = $deliveredCurrency, " : "")
                + (params.containsKey("amountCurrencyIssuer") ? " payment.amountCurrencyIssuer = $amountCurrencyIssuer, " : "")
                + (params.containsKey("amountCurrency") ? " payment.amountCurrency = $amountCurrency, " : "")
                + (params.containsKey("amount") ? " payment.amount = $amount" : ""), params);

        runQuery("MATCH (ledger:Ledger { ledgerIndex: $ledgerIndex })\n "
                + " MATCH (payment:Payment { hash: $hash})\n "
                + " MERGE (ledger)-[:CONTAINS]-(payment)", params);

        System.out.println(params.get("sender") + " pays " + params.get("receiver") + " " + params.get("amount") + " " + params.get("amountCurrency"));
    }

    private Result runQuery(String query, Map<String, Object> params) {
        Result result = null;
        try (Session session = neo4jDriver.session(SessionConfig.forDatabase(database))) {
            Query q = new Query(query, params);
            result = session.run(q);
        }
        return result;
    }

    private void createChildWalletAndActivation(LedgerResult ledgerResult, TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction> tr) {
        for (AffectedNode node : tr.metadata().get().affectedNodes()) {
            if (node instanceof CreatedNode) {
                if (node.ledgerEntryType().equals("AccountRoot")) {
                    long amount = ((ImmutablePayment) tr.transaction()).amount().map(
                            (t) -> {
                                // If it's XRP
                                return t.value().longValue();
                            },
                            (t) -> {
                                // If it's issued (should not happen? account creation without supplying xrp reserve would fail?)
                                return 0l;
                            }
                    );
                    String childWalletAddress = ((CreatedNode) node).newFields().account().get().value();
                    createWalletNode(childWalletAddress);

                    Map<String, Object> p = new HashMap<>();
                    p.put("parent", tr.transaction().account().toString());
                    p.put("child", ((CreatedNode) node).newFields().account().get().toString());
                    p.put("date", ledgerResult.ledger().closeTimeHuman().orElse(null));
                    p.put("hash", tr.hash().toString());
                    p.put("ledgerIndex", ledgerResult.ledgerIndexSafe().unsignedLongValue().longValue());
                    p.put("amount", amount);
                    createActivation(p);
                }
            }
        }
    }

    private List<TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction>> paymentsFromTransactions(List<TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction>> transactions) {
        return transactions.stream().filter((t) -> {
            Optional<TransactionMetadata> o = t.metadata();
            TransactionMetadata meta = o.orElse(null);
            return (meta != null && meta.transactionResult().equals("tesSUCCESS")) && (t.transaction().transactionType().equals(TransactionType.PAYMENT));
        }).collect(Collectors.toList());
    }

    private Stream<TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction>> activationsFromPayments(List<TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction>> payments) {
        return payments.stream().filter((t) -> {
            return isActivationPayment(t);
        });
    }

    private boolean isActivationPayment(TransactionResult<? extends org.xrpl.xrpl4j.model.transactions.Transaction> t) {
        TransactionMetadata meta = t.metadata().get();
        for (AffectedNode node : meta.affectedNodes()) {
            if (node instanceof CreatedNode) {
                if (node.ledgerEntryType().equals("AccountRoot")) {
                    NewFields newFields = ((CreatedNode) node).newFields();
                    return newFields != null && newFields.account().isPresent();
                }
            }
        }
        return false;
    }

    private void createWalletNode(String address) {
        runQuery("MERGE (wallet:Wallet { address: $address } ) ON CREATE SET wallet.address = $address", Collections.singletonMap("address", address));
        System.out.println("Created wallet " + address);
    }

    private void createLedgerNode(LedgerHeader ledger) {
        Map<String, Object> params = new HashMap<>();
        params.put("ledgerIndex", ledger.ledgerIndex().unsignedIntegerValue().longValue());
        params.put("ledgerHash", ledger.ledgerHash().orElseThrow(null).value());
        params.put("closeTime", ledger.closeTimeHuman().orElseThrow(null));
        params.put("parentHash", ledger.parentHash().value());
        params.put("totalCoins", ledger.totalCoins().orElseThrow(null).value().longValue());
        createLedgerNode(params);
    }

    private void createLedgerNode(Map<String, Object> params) {
        runQuery("MERGE (ledger:Ledger { ledgerIndex: $ledgerIndex })"
                + " ON CREATE SET "
                + "ledger.ledgerIndex = $ledgerIndex, "
                + "ledger.ledgerHash = $ledgerHash, "
                + "ledger.closeTime = datetime($closeTime), "
                + "ledger.parentHash = $parentHash, "
                + "ledger.totalCoins = $totalCoins"
                + " RETURN ledger",
                params);
        System.out.println("Created ledger " + params.get("ledgerIndex").toString() + " ( " + params.get("closeTime") + " )");
    }

    private void createActivation(Map<String, Object> params) {
        runQuery("MATCH (parent:Wallet { address: $parent })\n "
                + "MATCH (child:Wallet { address: $child })\n "
                + "MATCH (ledger:Ledger { ledgerIndex: $ledgerIndex })\n "
                + "MERGE (parent)-[activation:ACTIVATES]->(child)<-[:ACTIVATES]-(ledger)\n "
                + " ON CREATE SET\n "
                + " activation.date = datetime($date), "
                + " activation.hash = $hash, "
                + " activation.ledgerIndex = $ledgerIndex, "
                + " activation.amount = $amount", params);
        System.out.println(params.get("parent")
                + " activates " + params.get("child")
                + " with " + params.get("amount")
                + " in ledger " + params.get("ledgerIndex"));
    }

    public static void main(String[] args) throws URISyntaxException, InterruptedException, IOException, JsonRpcClientErrorException {
        try (Main m = new Main("bolt://localhost:7687", "user", "password", "database", Paths.get("/cachePath/"))) {
            m.run();
        }
    }

}
