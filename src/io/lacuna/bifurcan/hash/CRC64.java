package io.lacuna.bifurcan.hash;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public class CRC64 implements Checksum {

  private static final long[] TABLE = new long[]{
      0x0000000000000000L, 0x42f0e1eba9ea3693L, 0x85e1c3d753d46d26L, 0xc711223cfa3e5bb5L,
      0x493366450e42ecdfL, 0x0bc387aea7a8da4cL, 0xccd2a5925d9681f9L, 0x8e224479f47cb76aL,
      0x9266cc8a1c85d9beL, 0xd0962d61b56fef2dL, 0x17870f5d4f51b498L, 0x5577eeb6e6bb820bL,
      0xdb55aacf12c73561L, 0x99a54b24bb2d03f2L, 0x5eb4691841135847L, 0x1c4488f3e8f96ed4L,
      0x663d78ff90e185efL, 0x24cd9914390bb37cL, 0xe3dcbb28c335e8c9L, 0xa12c5ac36adfde5aL,
      0x2f0e1eba9ea36930L, 0x6dfeff5137495fa3L, 0xaaefdd6dcd770416L, 0xe81f3c86649d3285L,
      0xf45bb4758c645c51L, 0xb6ab559e258e6ac2L, 0x71ba77a2dfb03177L, 0x334a9649765a07e4L,
      0xbd68d2308226b08eL, 0xff9833db2bcc861dL, 0x388911e7d1f2dda8L, 0x7a79f00c7818eb3bL,
      0xcc7af1ff21c30bdeL, 0x8e8a101488293d4dL, 0x499b3228721766f8L, 0x0b6bd3c3dbfd506bL,
      0x854997ba2f81e701L, 0xc7b97651866bd192L, 0x00a8546d7c558a27L, 0x4258b586d5bfbcb4L,
      0x5e1c3d753d46d260L, 0x1cecdc9e94ace4f3L, 0xdbfdfea26e92bf46L, 0x990d1f49c77889d5L,
      0x172f5b3033043ebfL, 0x55dfbadb9aee082cL, 0x92ce98e760d05399L, 0xd03e790cc93a650aL,
      0xaa478900b1228e31L, 0xe8b768eb18c8b8a2L, 0x2fa64ad7e2f6e317L, 0x6d56ab3c4b1cd584L,
      0xe374ef45bf6062eeL, 0xa1840eae168a547dL, 0x66952c92ecb40fc8L, 0x2465cd79455e395bL,
      0x3821458aada7578fL, 0x7ad1a461044d611cL, 0xbdc0865dfe733aa9L, 0xff3067b657990c3aL,
      0x711223cfa3e5bb50L, 0x33e2c2240a0f8dc3L, 0xf4f3e018f031d676L, 0xb60301f359dbe0e5L,
      0xda050215ea6c212fL, 0x98f5e3fe438617bcL, 0x5fe4c1c2b9b84c09L, 0x1d14202910527a9aL,
      0x93366450e42ecdf0L, 0xd1c685bb4dc4fb63L, 0x16d7a787b7faa0d6L, 0x5427466c1e109645L,
      0x4863ce9ff6e9f891L, 0x0a932f745f03ce02L, 0xcd820d48a53d95b7L, 0x8f72eca30cd7a324L,
      0x0150a8daf8ab144eL, 0x43a04931514122ddL, 0x84b16b0dab7f7968L, 0xc6418ae602954ffbL,
      0xbc387aea7a8da4c0L, 0xfec89b01d3679253L, 0x39d9b93d2959c9e6L, 0x7b2958d680b3ff75L,
      0xf50b1caf74cf481fL, 0xb7fbfd44dd257e8cL, 0x70eadf78271b2539L, 0x321a3e938ef113aaL,
      0x2e5eb66066087d7eL, 0x6cae578bcfe24bedL, 0xabbf75b735dc1058L, 0xe94f945c9c3626cbL,
      0x676dd025684a91a1L, 0x259d31cec1a0a732L, 0xe28c13f23b9efc87L, 0xa07cf2199274ca14L,
      0x167ff3eacbaf2af1L, 0x548f120162451c62L, 0x939e303d987b47d7L, 0xd16ed1d631917144L,
      0x5f4c95afc5edc62eL, 0x1dbc74446c07f0bdL, 0xdaad56789639ab08L, 0x985db7933fd39d9bL,
      0x84193f60d72af34fL, 0xc6e9de8b7ec0c5dcL, 0x01f8fcb784fe9e69L, 0x43081d5c2d14a8faL,
      0xcd2a5925d9681f90L, 0x8fdab8ce70822903L, 0x48cb9af28abc72b6L, 0x0a3b7b1923564425L,
      0x70428b155b4eaf1eL, 0x32b26afef2a4998dL, 0xf5a348c2089ac238L, 0xb753a929a170f4abL,
      0x3971ed50550c43c1L, 0x7b810cbbfce67552L, 0xbc902e8706d82ee7L, 0xfe60cf6caf321874L,
      0xe224479f47cb76a0L, 0xa0d4a674ee214033L, 0x67c58448141f1b86L, 0x253565a3bdf52d15L,
      0xab1721da49899a7fL, 0xe9e7c031e063acecL, 0x2ef6e20d1a5df759L, 0x6c0603e6b3b7c1caL,
      0xf6fae5c07d3274cdL, 0xb40a042bd4d8425eL, 0x731b26172ee619ebL, 0x31ebc7fc870c2f78L,
      0xbfc9838573709812L, 0xfd39626eda9aae81L, 0x3a28405220a4f534L, 0x78d8a1b9894ec3a7L,
      0x649c294a61b7ad73L, 0x266cc8a1c85d9be0L, 0xe17dea9d3263c055L, 0xa38d0b769b89f6c6L,
      0x2daf4f0f6ff541acL, 0x6f5faee4c61f773fL, 0xa84e8cd83c212c8aL, 0xeabe6d3395cb1a19L,
      0x90c79d3fedd3f122L, 0xd2377cd44439c7b1L, 0x15265ee8be079c04L, 0x57d6bf0317edaa97L,
      0xd9f4fb7ae3911dfdL, 0x9b041a914a7b2b6eL, 0x5c1538adb04570dbL, 0x1ee5d94619af4648L,
      0x02a151b5f156289cL, 0x4051b05e58bc1e0fL, 0x87409262a28245baL, 0xc5b073890b687329L,
      0x4b9237f0ff14c443L, 0x0962d61b56fef2d0L, 0xce73f427acc0a965L, 0x8c8315cc052a9ff6L,
      0x3a80143f5cf17f13L, 0x7870f5d4f51b4980L, 0xbf61d7e80f251235L, 0xfd913603a6cf24a6L,
      0x73b3727a52b393ccL, 0x31439391fb59a55fL, 0xf652b1ad0167feeaL, 0xb4a25046a88dc879L,
      0xa8e6d8b54074a6adL, 0xea16395ee99e903eL, 0x2d071b6213a0cb8bL, 0x6ff7fa89ba4afd18L,
      0xe1d5bef04e364a72L, 0xa3255f1be7dc7ce1L, 0x64347d271de22754L, 0x26c49cccb40811c7L,
      0x5cbd6cc0cc10fafcL, 0x1e4d8d2b65facc6fL, 0xd95caf179fc497daL, 0x9bac4efc362ea149L,
      0x158e0a85c2521623L, 0x577eeb6e6bb820b0L, 0x906fc95291867b05L, 0xd29f28b9386c4d96L,
      0xcedba04ad0952342L, 0x8c2b41a1797f15d1L, 0x4b3a639d83414e64L, 0x09ca82762aab78f7L,
      0x87e8c60fded7cf9dL, 0xc51827e4773df90eL, 0x020905d88d03a2bbL, 0x40f9e43324e99428L,
      0x2cffe7d5975e55e2L, 0x6e0f063e3eb46371L, 0xa91e2402c48a38c4L, 0xebeec5e96d600e57L,
      0x65cc8190991cb93dL, 0x273c607b30f68faeL, 0xe02d4247cac8d41bL, 0xa2dda3ac6322e288L,
      0xbe992b5f8bdb8c5cL, 0xfc69cab42231bacfL, 0x3b78e888d80fe17aL, 0x7988096371e5d7e9L,
      0xf7aa4d1a85996083L, 0xb55aacf12c735610L, 0x724b8ecdd64d0da5L, 0x30bb6f267fa73b36L,
      0x4ac29f2a07bfd00dL, 0x08327ec1ae55e69eL, 0xcf235cfd546bbd2bL, 0x8dd3bd16fd818bb8L,
      0x03f1f96f09fd3cd2L, 0x41011884a0170a41L, 0x86103ab85a2951f4L, 0xc4e0db53f3c36767L,
      0xd8a453a01b3a09b3L, 0x9a54b24bb2d03f20L, 0x5d45907748ee6495L, 0x1fb5719ce1045206L,
      0x919735e51578e56cL, 0xd367d40ebc92d3ffL, 0x1476f63246ac884aL, 0x568617d9ef46bed9L,
      0xe085162ab69d5e3cL, 0xa275f7c11f7768afL, 0x6564d5fde549331aL, 0x279434164ca30589L,
      0xa9b6706fb8dfb2e3L, 0xeb46918411358470L, 0x2c57b3b8eb0bdfc5L, 0x6ea7525342e1e956L,
      0x72e3daa0aa188782L, 0x30133b4b03f2b111L, 0xf7021977f9cceaa4L, 0xb5f2f89c5026dc37L,
      0x3bd0bce5a45a6b5dL, 0x79205d0e0db05dceL, 0xbe317f32f78e067bL, 0xfcc19ed95e6430e8L,
      0x86b86ed5267cdbd3L, 0xc4488f3e8f96ed40L, 0x0359ad0275a8b6f5L, 0x41a94ce9dc428066L,
      0xcf8b0890283e370cL, 0x8d7be97b81d4019fL, 0x4a6acb477bea5a2aL, 0x089a2aacd2006cb9L,
      0x14dea25f3af9026dL, 0x562e43b4931334feL, 0x913f6188692d6f4bL, 0xd3cf8063c0c759d8L,
      0x5dedc41a34bbeeb2L, 0x1f1d25f19d51d821L, 0xd80c07cd676f8394L, 0x9afce626ce85b507L,
  };

  private long value = 0;

  public void update(ByteBuffer buffer) {
    while (buffer.hasRemaining()) {
      update(buffer.get() & 0xFF);
    }
  }

  @Override
  public void update(int b) {
    int t = (int) (((value >> 56) ^ b) & 0xFF);
    value = TABLE[t] ^ (value << 8);
  }

  @Override
  public void update(byte[] b, int off, int len) {
    for (int i = 0; i < len; i++) {
      update(b[off + i] & 0xFF);
    }
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public void reset() {
    value = 0;
  }
}
