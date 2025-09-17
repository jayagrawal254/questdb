/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test;

import io.questdb.QueryProbes;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class QueryProbesTest {

    @Test
    public void testHashSql() {
        // Test basic SQL hashing
        String sql1 = "SELECT * FROM table1";
        String sql2 = "SELECT * FROM table2";
        String sql3 = "SELECT * FROM table1";  // Same as sql1

        long hash1 = QueryProbes.hashSql(sql1);
        long hash2 = QueryProbes.hashSql(sql2);
        long hash3 = QueryProbes.hashSql(sql3);

        // Same SQL should produce same hash
        Assert.assertEquals(hash1, hash3);

        // Different SQL should produce different hashes (very likely)
        Assert.assertNotEquals(hash1, hash2);

        // Non-zero hashes for non-empty strings
        Assert.assertNotEquals(0L, hash1);
        Assert.assertNotEquals(0L, hash2);
    }

    @Test
    public void testHashSqlEdgeCases() {
        // Test edge cases
        Assert.assertEquals(0L, QueryProbes.hashSql(null));
        Assert.assertNotEquals(0L, QueryProbes.hashSql(""));

        // Test with various characters
        long hashSpecial = QueryProbes.hashSql("SELECT * FROM 'table' WHERE id = 123");
        Assert.assertNotEquals(0L, hashSpecial);

        // Test with Unicode
        long hashUnicode = QueryProbes.hashSql("SELECT 'héllo wörld' FROM table");
        Assert.assertNotEquals(0L, hashUnicode);
    }

    @Test
    public void testProbeMethodsWithoutNativeLibrary() {
        // These should not throw when native library is not available
        // Calls will be no-ops if USDT is disabled

        QueryProbes.start(12345L, QueryProbes.hashSql("SELECT 1"));
        QueryProbes.end(12345L);

        // Test with various query IDs
        for (long qid = 1; qid <= 10; qid++) {
            QueryProbes.start(qid, QueryProbes.hashSql("SELECT " + qid));
            QueryProbes.end(qid);
        }
    }

    @Test
    public void testIsEnabledConsistent() {
        // isEnabled() should return consistent results
        boolean enabled1 = QueryProbes.isEnabled();
        boolean enabled2 = QueryProbes.isEnabled();

        Assert.assertEquals(enabled1, enabled2);
    }

    @Test
    public void testHashConsistency() {
        // Hash should be consistent across calls
        String sql = "SELECT COUNT(*) FROM trades WHERE timestamp > now() - interval '1 hour'";

        long hash1 = QueryProbes.hashSql(sql);
        long hash2 = QueryProbes.hashSql(sql);
        long hash3 = QueryProbes.hashSql(sql);

        Assert.assertEquals(hash1, hash2);
        Assert.assertEquals(hash2, hash3);
    }

    @Test
    public void testHashDistribution() {
        // Generate many hashes to check basic distribution properties
        // This is not a rigorous statistical test, just a sanity check

        java.util.Set<Long> hashes = new java.util.HashSet<>();

        for (int i = 0; i < 1000; i++) {
            String sql = "SELECT * FROM table" + i + " WHERE id = " + (i * 7);
            long hash = QueryProbes.hashSql(sql);
            hashes.add(hash);
        }

        // Should have many distinct hashes (collisions are possible but rare)
        Assert.assertTrue("Hash function should produce diverse values",
                         hashes.size() > 950);  // Allow some collisions
    }

    @Test
    public void testLargeQueryHashing() {
        // Test with a large SQL query
        StringBuilder largeSql = new StringBuilder("SELECT ");
        for (int i = 0; i < 1000; i++) {
            largeSql.append("col").append(i);
            if (i < 999) largeSql.append(", ");
        }
        largeSql.append(" FROM large_table");

        long hash = QueryProbes.hashSql(largeSql.toString());
        Assert.assertNotEquals(0L, hash);

        // Should be consistent
        long hash2 = QueryProbes.hashSql(largeSql.toString());
        Assert.assertEquals(hash, hash2);
    }
}