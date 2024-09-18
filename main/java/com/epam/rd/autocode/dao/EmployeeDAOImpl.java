package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EmployeeDAOImpl implements EmployeeDao {

    private final String INSERT = "INSERT INTO employee (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, manager, hiredate, salary, department) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private final String UPDATE = "UPDATE employee SET ID=?, firstname=?, lastname=?, middlename=?, position=?, manager=?, hiredate=?, salary=?, department=? WHERE ID=?";
    private final String SELECT = "SELECT ID, firstname, lastname, middlename, position, manager, hiredate, salary, department FROM employee";
    private final String SELECT_ID = "SELECT ID, firstname, lastname, middlename, position, manager, hiredate, salary, department FROM employee WHERE ID=?";
    private final String SELECT_DEPARTMENT_ID = "SELECT ID, firstname, lastname, middlename, position, manager, hiredate, salary, department FROM employee WHERE department=?";
    private final String SELECT_MANAGER_ID = "SELECT ID, firstname, lastname, middlename, position, manager, hiredate, salary, department FROM employee WHERE manager=?";
    private final String DELETE = "DELETE FROM Employee WHERE ID=?";

    private Connection connection;

    public EmployeeDAOImpl() {
        try {
            connection = ConnectionSource.instance().createConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<Employee> getById(BigInteger Id) {
        if (connection == null) {
            return Optional.empty();
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ID)) {
            preparedStatement.setInt(1, Id.intValue());
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                BigInteger currId = BigInteger.valueOf(resultSet.getInt(1));
                if (currId.equals(Id)) {
                    String firstname = resultSet.getString("firstname");
                    String lastname = resultSet.getString("lastname");
                    String middleName = resultSet.getString("middlename");
                    FullName fullName = new FullName(firstname, lastname, middleName);
                    Position position = Position.valueOf(resultSet.getString("position"));
                    LocalDate hired = resultSet.getDate(7).toLocalDate();
                    BigDecimal salary = resultSet.getBigDecimal("salary");
                    BigInteger managerId = BigInteger.valueOf(resultSet.getInt(6));
                    BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(9));
                    Employee employee = new Employee(currId, fullName, position, hired, salary, managerId, departmentId);
                    return Optional.of(employee);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> list = new ArrayList<>();
        if (connection == null) {
            return null;
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                BigInteger currId = BigInteger.valueOf(resultSet.getInt(1));
                String firstname = resultSet.getString("FIRSTNAME");
                String lastname = resultSet.getString("LASTNAME");
                String middleName = resultSet.getString("MIDDLENAME");
                FullName fullName = new FullName(firstname, lastname, middleName);
                Position position = Position.valueOf(resultSet.getString("POSITION"));
                LocalDate hired = resultSet.getDate(7).toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("salary");
                BigInteger managerId = BigInteger.valueOf(resultSet.getInt(6));
                BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(9));
                Employee employee = new Employee(currId, fullName, position, hired, salary, managerId, departmentId);
                list.add(employee);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public Employee save(Employee employee) {
        if (connection == null) {
            return null;
        }
        try {
            Employee employee1 = getById(employee.getId()).orElse(null);
            PreparedStatement preparedStatement;
            if (employee1 == null) {
                preparedStatement = connection.prepareStatement(INSERT);
                preparedStatement.setInt(1, employee.getId().intValue());
                preparedStatement.setString(2, employee.getFullName().getFirstName());
                preparedStatement.setString(3, employee.getFullName().getLastName());
                preparedStatement.setString(4, employee.getFullName().getMiddleName());
                preparedStatement.setString(5, employee.getPosition().toString());
                preparedStatement.setInt(6, employee.getManagerId().intValue());
                preparedStatement.setDate(7, Date.valueOf(employee.getHired()));
                preparedStatement.setDouble(8, employee.getSalary().doubleValue());
                preparedStatement.setInt(9, employee.getDepartmentId().intValue());
            } else {
                preparedStatement = connection.prepareStatement(UPDATE);
                preparedStatement.setInt(1, employee.getId().intValue());
                preparedStatement.setString(2, employee.getFullName().getFirstName());
                preparedStatement.setString(3, employee.getFullName().getLastName());
                preparedStatement.setString(4, employee.getFullName().getMiddleName());
                preparedStatement.setString(5, employee.getPosition().toString());
                preparedStatement.setInt(6, employee.getManagerId().intValue());
                preparedStatement.setDate(7, Date.valueOf(employee.getHired()));
                preparedStatement.setDouble(8, employee.getSalary().doubleValue());
                preparedStatement.setInt(9, employee.getDepartmentId().intValue());
                preparedStatement.setInt(10, employee.getId().intValue());
            }
            preparedStatement.execute();
            employee1 = getById(employee.getId()).orElse(null);
            return employee1;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employee;
    }

    @Override
    public void delete(Employee employee) {
        if (connection == null) {
            return;
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(DELETE)) {
            preparedStatement.setInt(1, employee.getId().intValue());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> list = new ArrayList<>();
        if (connection == null) {
            return null;
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_DEPARTMENT_ID)) {
            preparedStatement.setInt(1, department.getId().intValue());
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                BigInteger currId = BigInteger.valueOf(resultSet.getInt(1));
                String firstname = resultSet.getString("firstName");
                String lastname = resultSet.getString("lastName");
                String middleName = resultSet.getString("middleName");
                FullName fullName = new FullName(firstname, lastname, middleName);
                Position position = Position.valueOf(resultSet.getString("position"));
                LocalDate hired = resultSet.getDate(7).toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("salary");
                BigInteger managerId = BigInteger.valueOf(resultSet.getInt(6));
                BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(9));
                Employee employee = new Employee(currId, fullName, position, hired, salary, managerId, departmentId);
                list.add(employee);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        List<Employee> list = new ArrayList<>();
        if (connection == null) {
            return null;
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_MANAGER_ID)) {
            preparedStatement.setInt(1, employee.getId().intValue());
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                BigInteger currId = BigInteger.valueOf(resultSet.getInt(1));
                String firstname = resultSet.getString("firstName");
                String lastname = resultSet.getString("lastName");
                String middleName = resultSet.getString("middleName");
                FullName fullName = new FullName(firstname, lastname, middleName);
                Position position = Position.valueOf(resultSet.getString("position"));
                LocalDate hired = resultSet.getDate(7).toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("salary");
                BigInteger managerId = BigInteger.valueOf(resultSet.getInt(6));
                BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(9));
                Employee employee1 = new Employee(currId, fullName, position, hired, salary, managerId, departmentId);
                list.add(employee1);
            }
            return list;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}
