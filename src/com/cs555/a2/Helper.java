package com.cs555.a2;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Random;

class Helper {
    static Random rng = new Random(Instant.now().toEpochMilli());
    private final static int BpID = 16;
    final static int BpH = 4; //number of bits per hex
    final static int HpID = BpID / BpH; //number of hex chars per ID
    static String peerStoragePath = "/tmp/dwhite54/peerstorage";

    static char GenerateID() {
        return (char)rng.nextInt();
    }

    static char getDigest(byte[] input) throws NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("MD5");
        crypt.reset();
        crypt.update(input);
        return ByteBuffer.wrap(crypt.digest()).getChar();
    }

    //length of longest common prefix
    //returns -1 if no match
    static int getLongestCommonPrefixLength(char ID1, char ID2) {
        int mask = 0;
        for (int i = 0; i < HpID; i++) {
            int shift = (HpID - i - 1) * BpH;
            mask += (0xf << shift);
            if ((ID1 & mask) != (ID2 & mask))
                return i;
        }
        return HpID;
    }

    //return hex/integer value at ith position in hex string of ID
    static int getValueAtHexIdx(char ID, int i) {
        if (i >= Helper.HpID) throw new IndexOutOfBoundsException();
        int shift = (Helper.HpID - i - 1) * Helper.BpH;
        int mask = 0xf << shift;
        return (mask & ID) >> shift;
    }

    static String getNickname(char ID) {
        int idx = ID % nicknames.length;
        return nicknames[idx];
    }

    // distance from ID1 to ID2, preserving sign (negative means ID1 clockwise of ID2 on clockwise-increasing ring)
    static int ringDistance(char a, char b) {
        int simple = Math.abs(a-b);
        return Integer.min(simple, Character.MAX_VALUE + 1 - simple);
    }

    static boolean isBetween(char lower, char test, char upper) {
        if (lower == upper)
            return true;
        else if (lower < upper)
            return (lower <= test && test <= upper);
        else // lower > upper, i.e. overflow boundary is between
            return (lower <= test || test <= upper);
    }

    private static final String[] nicknames = {"Abby", "Abe", "Abram", "Aby", "Ada", "Adrian", "Aggie", "Aggy",
            "Aileen", "Ailie", "Alan", "Alec", "Aleck", "Alex", "Alf", "Alfie", "Algy", "Alick", "Allie", "Ally",
            "Andy", "Ann", "Anna", "Anne", "Annabel", "Annaple", "Annie", "Archie", "Archy", "Augustin", "Austin",
            "Bab", "Babbie", "Babs", "Baldie", "Barnabas", "Barnaby", "Barney", "Bart", "Bat", "Beatrice", "Beatrix",
            "Beck", "Becky", "Bel", "Bella", "Belle", "Ben", "Bennet", "Benny", "Bert", "Bertie", "Berty", "Bess",
            "Bessie", "Bessy", "Beth", "Betsy", "Betty", "Bex", "Biddy", "Bill", "Billie", "Billy", "Bob", "Bobby",
            "Caddie", "Caitlin", "Carrie", "Cassie", "Casy", "Cate", "Cathie", "Charley", "Chas", "Charlie", "Chris",
            "Chrissie", "Christie", "Christy", "Chuck", "Cindy", "Cis", "Cissie", "Cissy", "Claire", "Clare", "Claud",
            "Clement", "Connie", "Corinna", "Crispin", "Crispus", "Dan", "Danny", "Dave", "Davy", "Denis", "Di", "Dick",
            "Dicken", "Dickie", "Dickon", "Dicky", "Die", "Dob", "Dobbin", "Dod", "Doddy", "Dol", "Dolly", "Dora",
            "Dot", "Eck", "Ecky", "Ed", "Eddie", "Eddy", "Edie", "Effie", "Eileen", "Ella", "Ellen", "Elsie", "Elspie",
            "Emm", "Emmie", "Eneas", "Essie", "Etta", "Euphie", "Eva", "Eve", "Fanny", "Flo", "Flossie", "Floy",
            "Francie", "Frank", "Frankie", "Fred", "Freddie", "Freddy", "Gabe", "Genie", "Georgie", "Gertie", "Gil",
            "Greg", "Grissel", "Gritty", "Gus", "Gussie", "Gustus", "Hal", "Harry", "Hatty", "Hen", "Henny", "Hetty",
            "Hodge", "Hodgekin", "Horace", "Huggin", "Hughie", "Hughoc", "Humph", "Hy", "Ik", "Ike", "Inez", "Isa",
            "Jack", "Jake", "Janet", "Jeames", "Jean", "Jeanie", "Jeannie", "Jem", "Jemmy", "Jen", "Jennie", "Jenny",
            "Jeremy", "Jess", "Jessie", "Jim", "Jimmie", "Jimmy", "Jo", "Jock", "Joe", "Joey", "Johnnie", "Johnny",
            "Josh", "Josie", "Josy", "Jozy", "Judy", "Jule", "Jules", "Julie", "Kate", "Kath", "Kathie", "Katie",
            "Katrine", "Ken", "Kester", "Kit", "Kitty", "Larry", "Laura", "Len", "Lena", "Lenny", "Lettie", "Lew",
            "Lewie", "Lex", "Lexie", "Libby", "Lilly", "Lily", "Lisa", "Liz", "Liza", "Lizzie", "Lorrie", "Lou",
            "Louie", "Lucy", "Luke", "Maddie", "Madge", "Mag", "Maggie", "Mamie", "Manny", "Margery", "Margie",
            "Marjorie", "Marjory", "Mark", "Mat", "Matt", "Mattie", "Matty", "Maud", "Maudlin", "May", "Meg", "Meggy",
            "Meta", "Mick", "Micky", "Mif", "Miffy", "Mike", "Mina", "Minella", "Minnie", "Moll", "Molly", "Mose",
            "Mosey", "Nabby", "Nan", "Nance", "Nancy", "Nanny", "Nat", "Ned", "Neddy", "Nell", "Nellie", "Net",
            "Nettie", "Netty", "Nic", "Nick", "Nina", "Nol", "Nolly", "Nora", "Norah", "Ollie", "Olly", "Paddy", "Pat",
            "Patty", "Paul", "Peg", "Peggy", "Penny", "Pete", "Peterkin", "Phamie", "Phemie", "Pheny", "Phil", "Phyl",
            "Pip", "Pippa", "Pol", "Polly", "Prudy", "Prue", "Ralph", "Ray", "Reg", "Reggie", "Rick", "Rob", "Robbie",
            "Robin", "Rod", "Roddie", "Roddy", "Rodge", "Rosie", "Roxy", "Sal", "Sally", "Sam", "Sammy", "Sanders",
            "Sandy", "Sawnie", "Seb", "Sibella", "Sim", "Sis", "Sisely", "Sissie", "Sly", "Sol", "Sophie", "Sophy",
            "Steenie", "Steve", "Stevie", "Sue", "Suke", "Suky", "Susie", "Susy", "Tam", "Tammie", "Tave", "Tavy",
            "Ted", "Teddy", "Teenie", "Terry", "Tib", "Tibbie", "Tilda", "Tillie", "Tim", "Tina", "Toby", "Tom",
            "Tommy", "Tony", "Tracie", "Trudy", "Vest", "Vester", "Vic", "Vicky", "Vinty", "Wat", "Watty", "Will",
            "Willie", "Willy", "Wilmett", "Wilmot", "Winnie", "Xina", "Yiddy", "Zach", "Zacky", "Zak", "Zeke"};
}
