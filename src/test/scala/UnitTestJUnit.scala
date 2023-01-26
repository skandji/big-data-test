import org.junit.Test
import junit.framework.TestCase.assertEquals

class UnitTestJUnit {

  @Test
  def testDivise(): Unit = {
    var valAc = HelloWorld.divide(24, 12)
    var valAt = 5
    assertEquals(s"La valeur attendue de la division est ${valAt}", valAc, valAt)
  }
}
