/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.authentication;

import org.testng.annotations.Test;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import java.net.InetAddress;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;

public class KerberosTicketUtilsTest
{
    @Test
    public void testGetTicketGrantingTicket() throws Exception
    {
        // Setup
        final Subject subject = new Subject(false, new HashSet<>(), new HashSet<>(), new HashSet<>());
        final KerberosTicket expectedResult = new KerberosTicket("content".getBytes(), new KerberosPrincipal("name", 0),
                new KerberosPrincipal("name", 0), "content".getBytes(), 0, new boolean[]{false},
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new InetAddress[]{InetAddress.getByName("localhost")});

        // Run the test
        final KerberosTicket result = KerberosTicketUtils.getTicketGrantingTicket(subject);
    }

    @Test
    public void testGetRefreshTime() throws Exception
    {
        // Setup
        final KerberosTicket ticket = new KerberosTicket("content".getBytes(), new KerberosPrincipal("name", 0),
                new KerberosPrincipal("name", 0), "content".getBytes(), 0, new boolean[]{false},
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new InetAddress[]{InetAddress.getByName("localhost")});

        // Run the test
        final long result = KerberosTicketUtils.getRefreshTime(ticket);
    }

    @Test
    public void testIsOriginalTicketGrantingTicket() throws Exception
    {
        // Setup
        final KerberosTicket ticket = new KerberosTicket("content".getBytes(), new KerberosPrincipal("name", 0),
                new KerberosPrincipal("name", 0), "content".getBytes(), 0, new boolean[]{false},
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(),
                new InetAddress[]{InetAddress.getByName("localhost")});

        // Run the test
        final boolean result = KerberosTicketUtils.isOriginalTicketGrantingTicket(ticket);
    }
}
