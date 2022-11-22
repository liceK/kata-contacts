const sqlite3 = require('sqlite3')
const open = require('sqlite').open
const fs = require('fs')

const filename = 'contacts.sqlite3'
const myArgs = process.argv.slice(2)
const numContacts = myArgs.length ? myArgs[0] : 3

const shouldMigrate = !fs.existsSync(filename)

/**
 * Generate `numContacts` contacts,
 * one at a time
 *
 */
function * generateContacts (numContacts) {
  let i = 1
  while (i <= numContacts) {
    yield [`name-${i}`, `email-${i}@domain.tld`]
    i++
  }
}

const migrate = async (db) => {
  console.log('Migrating db ...')
  await db.exec(`
        CREATE TABLE contacts(
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL
         );
     `)
  //await db.exec(`CREATE UNIQUE INDEX index_contacts_email ON contacts(email);`)
  console.log('Done migrating db')
}

const insertContacts = async (db) => {
  const start = Date.now()
  console.log('Inserting contacts ...')
  let listContacts = []
  for (const contact of generateContacts(numContacts)) {
    listContacts.push(contact)
    if (listContacts.length == 10000) {
      let param = listContacts.map((contact) => '(?, ?)').join(',');
      const sql = 'INSERT INTO contacts(name, email) VALUES ' + param;
      await db.run(sql, listContacts.flat())
      listContacts = []
    }
  }
  if (listContacts.length) {
    let param = listContacts.map((contact) => '(?, ?)').join(',');
    const sql = 'INSERT INTO contacts(name, email) VALUES ' + param;
    await db.run(sql, listContacts.flat())
    listContacts = []
  }
  const end = Date.now()
  const elapsed = (end - start) / 1000
  console.log(`Insert took ${elapsed} seconds`)
}

const queryContact = async (db) => {
  const start = Date.now()
  const res = await db.get('SELECT name FROM contacts WHERE email = ?', [`email-${numContacts}@domain.tld`])
  if (!res || !res.name) {
    console.error('Contact not found')
    process.exit(1)
  }
  console.log(res.name)
  const end = Date.now()
  const elapsed = (end - start) / 1000
  console.log(`Query took ${elapsed} seconds`)
}

(async () => {
  const db = await open({
    filename,
    driver: sqlite3.Database
  })
  if (shouldMigrate) {
    await migrate(db)
  } else {
    await db.run(`DELETE FROM contacts`)
  }
  await insertContacts(db)
  await queryContact(db)
  await db.close()
})()
