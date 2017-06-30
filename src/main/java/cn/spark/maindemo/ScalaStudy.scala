package cn.spark.maindemo

/**
  * Created by Administrator on 2017/06/30.
  */
class ScalaStudy(name:String,var age:Int) {
    println("person")

//    def show(): Unit = {
//      println("show.."+name)
//    }
def apply()={
  println("class is print")
}
    var gender:String=_
//  var Sname:String=_

    //次构造器必须调用主构造器,参数不能使用var
    def this(name:String,age:Int, gender:String){
      this(name,age)
//      this.Sname=name
      this.gender=gender
    }
}
object  ScalaStudy {
  def main(args: Array[String]): Unit = {
    val scala=ScalaStudy()
    scala()
  }
  def apply()={
    print("Ojbec ScalaStudy is print")
    new ScalaStudy("历史",20)
  }
}