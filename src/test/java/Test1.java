import org.junit.Test;

/**
 * @Author: skm
 * @Date: 2019/4/27 18:01
 *github的更新操作
 */
public class Test1 {
    @Test
    public void test1() {
        Demo1.createTable("student5", "skm1,smk2,skm3,skm4");
    }

    @Test
    public void test2() {
        Demo1.dropTable("student");
    }
    @Test
    public void test3() {
        Demo1.addData("student", "1", "个人信息", "姓名", "王小波");
    }

    @Test
    public void test4() {
        Demo1.deleteMultiRow("student", "1");
    }
    @Test
    public void test5() {
        Demo1.getAllRows("student");
    }

}
