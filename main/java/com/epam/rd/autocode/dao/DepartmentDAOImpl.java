package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDAOImpl implements DepartmentDao {

    private final String INSERT = "INSERT INTO department (ID, name, location) VALUES (?, ?, ?)";
    private final String UPDATE = "UPDATE department SET ID=?, name=?, location=? WHERE ID=?";
    private final String SELECT = "SELECT ID, name, location FROM department";
    private final String SELECT_ID = "SELECT ID as ID, name, location FROM department WHERE ID=?";
    private final String DELETE = "DELETE FROM department WHERE ID=?";

    private Connection connection;

    public DepartmentDAOImpl() {
        try {
            connection = ConnectionSource.instance().createConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<Department> getById(BigInteger Id) {
        if (connection == null) {
            return Optional.empty();
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ID)) {
            preparedStatement.setInt(1, Id.intValue());
            ResultSet resultSet = preparedStatement.executeQuery();
            Department department = null;
            while (resultSet.next()) {
                BigInteger currId = BigInteger.valueOf(resultSet.getInt(1));
                if (currId.equals(Id)) {
                    String name = resultSet.getString(2);
                    String location = resultSet.getString(3);
                    department = new Department(currId, name, location);
                    return Optional.of(department);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public List<Department> getAll() {
        List<Department> list = new ArrayList<>();
        if (connection == null) {
            return null;
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                BigInteger currId = BigInteger.valueOf(resultSet.getInt(1));
                String name = resultSet.getString(2);
                String location = resultSet.getString(3);
                Department department = new Department(currId, name, location);
                list.add(department);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public Department save(Department department) {
        if (connection == null) {
            return null;
        }
        try {
            Department d = getById(department.getId()).orElse(null);
            if (d == null) {
                PreparedStatement preparedStatement = connection.prepareStatement(INSERT);
                preparedStatement.setInt(1, department.getId().intValue());
                preparedStatement.setString(2, department.getName());
                preparedStatement.setString(3, department.getLocation());
                preparedStatement.execute();
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement(UPDATE);
                preparedStatement.setInt(1, department.getId().intValue());
                preparedStatement.setString(2, department.getName());
                preparedStatement.setString(3, department.getLocation());
                preparedStatement.setInt(4, department.getId().intValue());
                preparedStatement.execute();
            }
            d = getById(department.getId()).get();
            return d;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return department;
    }

    @Override
    public void delete(Department department) {
        if (connection == null) {
            return;
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(DELETE)) {
            preparedStatement.setInt(1, department.getId().intValue());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
