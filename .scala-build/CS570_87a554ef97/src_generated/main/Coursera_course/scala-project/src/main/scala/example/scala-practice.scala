package Coursera_course.scala$minusproject.src.main.scala.example


final class scala$minuspractice$_ {
def args = scala$minuspractice_sc.args$
def scriptPath = """Coursera_course/scala-project/src/main/scala/example/scala-practice.sc"""
/*<script>*/
 val computed = {                                                                                                                         
                 val x = 10 
                 val y = 20 
                 x+y
                 }
/*</script>*/ /*<generated>*//*</generated>*/
}

object scala$minuspractice_sc {
  private var args$opt0 = Option.empty[Array[String]]
  def args$set(args: Array[String]): Unit = {
    args$opt0 = Some(args)
  }
  def args$opt: Option[Array[String]] = args$opt0
  def args$: Array[String] = args$opt.getOrElse {
    sys.error("No arguments passed to this script")
  }

  lazy val script = new scala$minuspractice$_

  def main(args: Array[String]): Unit = {
    args$set(args)
    val _ = script.hashCode() // hashCode to clear scalac warning about pure expression in statement position
  }
}

export scala$minuspractice_sc.script as `scala-practice`

